using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Sx.Communication
{
    public sealed class TcpClient : ITcpClient
    {
        private readonly TcpClientOptions _options;
        private readonly object _syncRoot = new object();
        private readonly SemaphoreSlim _sendLock = new SemaphoreSlim(1, 1);
        private System.Net.Sockets.TcpClient _socket;
        private NetworkStream _stream;
        private TcpClientState _state;
        private CancellationTokenSource _receiveCancellation;
        private Task _receiveTask;
        private bool _disposed;

        public TcpClient(TcpClientOptions options)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            options.Validate();
            ValidateEndpoint(options);
            _options = options;
        }

        public TcpClient(string host, int port)
            : this(new TcpClientOptions { Host = host, Port = port })
        {
        }

        public TcpClientState State
        {
            get
            {
                lock (_syncRoot)
                {
                    return _state;
                }
            }
        }

        public bool IsConnected
        {
            get { return State == TcpClientState.Connected; }
        }

        public event EventHandler<TcpDataReceivedEventArgs> DataReceived;
        public event EventHandler<TcpStateChangedEventArgs> StateChanged;
        public event EventHandler<TcpErrorEventArgs> ErrorOccurred;

        public async Task ConnectAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            ThrowIfDisposed();

            lock (_syncRoot)
            {
                if (_state == TcpClientState.Connected || _state == TcpClientState.Connecting)
                {
                    throw new InvalidOperationException("The TCP client is already connected or connecting.");
                }

                SetStateCore(TcpClientState.Connecting);
            }

            var socket = new System.Net.Sockets.TcpClient();
            var timeoutCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCancellation.CancelAfter(_options.ConnectTimeoutMilliseconds);

            try
            {
                var connectTask = socket.ConnectAsync(_options.Host, _options.Port);
                await WaitForTaskAsync(connectTask, cancellationToken, timeoutCancellation.Token).ConfigureAwait(false);

                lock (_syncRoot)
                {
                    ThrowIfDisposed();
                    _socket = socket;
                    _stream = socket.GetStream();
                    _receiveCancellation = new CancellationTokenSource();
                    SetStateCore(TcpClientState.Connected);
                    _receiveTask = ReceiveLoopAsync(_receiveCancellation.Token);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                socket.Close();
                lock (_syncRoot)
                {
                    if (!_disposed)
                    {
                        SetStateCore(TcpClientState.Disconnected);
                    }
                }

                throw;
            }
            catch (TimeoutException)
            {
                socket.Close();
                lock (_syncRoot)
                {
                    if (!_disposed)
                    {
                        SetStateCore(TcpClientState.Faulted);
                    }
                }

                throw;
            }
            catch
            {
                socket.Close();

                lock (_syncRoot)
                {
                    if (!_disposed)
                    {
                        SetStateCore(TcpClientState.Faulted);
                    }
                }

                throw;
            }
            finally
            {
                timeoutCancellation.Dispose();
            }
        }

        public async Task DisconnectAsync()
        {
            Task receiveTask;
            lock (_syncRoot)
            {
                ThrowIfDisposed();

                if (_state == TcpClientState.Disconnected)
                {
                    return;
                }

                SetStateCore(TcpClientState.Disconnecting);

                if (_receiveCancellation != null)
                {
                    _receiveCancellation.Cancel();
                }

                receiveTask = _receiveTask;
                CloseSocketCore();
                SetStateCore(TcpClientState.Disconnected);
            }

            if (receiveTask != null)
            {
                try
                {
                    await receiveTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        public async Task SendAsync(byte[] data, CancellationToken cancellationToken = default(CancellationToken))
        {
            ThrowIfDisposed();

            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            if (data.Length == 0)
            {
                return;
            }

            await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                ThrowIfDisposed();
                NetworkStream stream;
                lock (_syncRoot)
                {
                    if (!IsConnected)
                    {
                        throw new InvalidOperationException("The TCP client is not connected.");
                    }

                    stream = _stream;
                }

                await stream.WriteAsync(data, 0, data.Length, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }

        public void Dispose()
        {
            lock (_syncRoot)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                if (_receiveCancellation != null)
                {
                    _receiveCancellation.Cancel();
                }

                CloseSocketCore();
                SetStateCore(TcpClientState.Disconnected);
            }

            _receiveCancellation.Dispose();
            _sendLock.Dispose();
        }

        private async Task WaitForTaskAsync(Task task, CancellationToken callerCancellationToken, CancellationToken timeoutCancellationToken)
        {
            var timeoutTask = Task.Delay(Timeout.Infinite, timeoutCancellationToken);
            var completed = await Task.WhenAny(task, timeoutTask).ConfigureAwait(false);

            if (completed == timeoutTask)
            {
                if (callerCancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException(callerCancellationToken);
                }

                throw new TimeoutException("The TCP connection attempt timed out.");
            }

            await task.ConfigureAwait(false);
        }

        private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
        {
            var buffer = new byte[_options.ReceiveBufferSize];

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    NetworkStream stream;
                    lock (_syncRoot)
                    {
                        stream = _stream;
                    }

                    if (stream == null)
                    {
                        return;
                    }

                    var count = await stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false);
                    if (count == 0)
                    {
                        HandleRemoteDisconnect();
                        return;
                    }

                    var received = new byte[count];
                    Buffer.BlockCopy(buffer, 0, received, 0, count);
                    OnDataReceived(received);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (IOException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception exception)
            {
                HandleReceiveFailure(exception);
            }
        }

        private void HandleRemoteDisconnect()
        {
            lock (_syncRoot)
            {
                if (_disposed || _state != TcpClientState.Connected)
                {
                    return;
                }

                CloseSocketCore();
                SetStateCore(TcpClientState.Disconnected);
            }
        }

        private void HandleReceiveFailure(Exception exception)
        {
            lock (_syncRoot)
            {
                if (_disposed || _state != TcpClientState.Connected)
                {
                    return;
                }

                CloseSocketCore();
                SetStateCore(TcpClientState.Faulted);
            }

            OnErrorOccurred(exception);
        }

        private void CloseSocketCore()
        {
            if (_stream != null)
            {
                _stream.Dispose();
                _stream = null;
            }

            if (_socket != null)
            {
                _socket.Close();
                _socket = null;
            }
        }

        private void SetStateCore(TcpClientState state)
        {
            if (_state == state)
            {
                return;
            }

            var previous = _state;
            _state = state;
            OnStateChanged(new TcpStateChangedEventArgs(state, previous));
        }

        private void OnDataReceived(byte[] data)
        {
            var handler = DataReceived;
            if (handler != null)
            {
                handler(this, new TcpDataReceivedEventArgs(data));
            }
        }

        private void OnStateChanged(TcpStateChangedEventArgs eventArgs)
        {
            var handler = StateChanged;
            if (handler != null)
            {
                handler(this, eventArgs);
            }
        }

        private void OnErrorOccurred(Exception exception)
        {
            var handler = ErrorOccurred;
            if (handler != null)
            {
                handler(this, new TcpErrorEventArgs(exception));
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
        }

        private static void ValidateEndpoint(TcpClientOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.Host))
            {
                throw new ArgumentException("Host cannot be empty.", nameof(options));
            }

            if (options.Port < 1 || options.Port > 65535)
            {
                throw new ArgumentOutOfRangeException(nameof(options));
            }
        }
    }
}
