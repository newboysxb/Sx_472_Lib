using System;

namespace Sx.Communication
{
    public sealed class TcpDataReceivedEventArgs : EventArgs
    {
        public TcpDataReceivedEventArgs(byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            Data = data;
        }

        public byte[] Data { get; }
    }
}
