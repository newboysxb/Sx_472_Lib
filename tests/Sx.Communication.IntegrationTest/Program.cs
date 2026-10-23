using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Sx.Communication;

namespace Sx.Communication.IntegrationTest
{
    internal static class Program
    {
        private static int Main()
        {
            try
            {
                RunAsync().GetAwaiter().GetResult();
                Console.WriteLine("Integration test passed.");
                return 0;
            }
            catch (Exception exception)
            {
                Console.Error.WriteLine(exception);
                return 1;
            }
        }

        private static async Task RunAsync()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            var serverTask = RunServerAsync(listener);

            var received = new TaskCompletionSource<byte[]>();
            using (var client = new TcpClient_sx(new TcpClientOptions
            {
                Host = IPAddress.Loopback.ToString(),
                Port = port
            }))
            {
                client.DataReceived += (sender, eventArgs) => received.TrySetResult(eventArgs.Data);
                await client.ConnectAsync();

                var payload = Encoding.ASCII.GetBytes("hello-472");
                await client.SendAsync(payload);
                var echoed = await Task.WhenAny(received.Task, Task.Delay(3000));
                if (echoed != received.Task)
                {
                    throw new TimeoutException("The client did not receive the echo.");
                }

                var result = Encoding.ASCII.GetString(received.Task.Result);
                if (result != "hello-472")
                {
                    throw new InvalidOperationException("Unexpected echoed payload: " + result);
                }

                await client.DisconnectAsync();
            }

            await serverTask;
        }

        private static async Task RunServerAsync(TcpListener listener)
        {
            try
            {
                using (var socket = await listener.AcceptTcpClientAsync())
                using (var stream = socket.GetStream())
                {
                    var buffer = new byte[64];
                    var count = await stream.ReadAsync(buffer, 0, buffer.Length);
                    if (Encoding.ASCII.GetString(buffer, 0, count) != "hello-472")
                    {
                        throw new InvalidOperationException("The server received an unexpected payload.");
                    }

                    await stream.WriteAsync(buffer, 0, count);
                    await Task.Delay(100);
                }
            }
            finally
            {
                listener.Stop();
            }
        }
    }
}
