using System;
using System.Threading;
using System.Threading.Tasks;

namespace Sx.Communication
{
    public interface ITcpClient : IDisposable
    {
        TcpClientState State { get; }

        bool IsConnected { get; }

        Task ConnectAsync(CancellationToken cancellationToken = default(CancellationToken));

        Task DisconnectAsync();

        Task SendAsync(byte[] data, CancellationToken cancellationToken = default(CancellationToken));

        event EventHandler<TcpDataReceivedEventArgs> DataReceived;

        event EventHandler<TcpStateChangedEventArgs> StateChanged;

        event EventHandler<TcpErrorEventArgs> ErrorOccurred;
    }
}
