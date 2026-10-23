using System;

namespace Sx.Communication
{
    public sealed class TcpClientOptions
    {
        public string Host { get; set; }

        public int Port { get; set; }

        public int ConnectTimeoutMilliseconds { get; set; } = 5000;

        public int SendTimeoutMilliseconds { get; set; } = 5000;

        public int ReceiveBufferSize { get; set; } = 4096;

        public void Validate()
        {
            if (ConnectTimeoutMilliseconds <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(ConnectTimeoutMilliseconds));
            }

            if (SendTimeoutMilliseconds <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(SendTimeoutMilliseconds));
            }

            if (ReceiveBufferSize <= 0 || ReceiveBufferSize > 1024 * 1024)
            {
                throw new ArgumentOutOfRangeException(nameof(ReceiveBufferSize));
            }
        }
    }
}
