using System;

namespace Sx.Communication
{
    public sealed class TcpErrorEventArgs : EventArgs
    {
        public TcpErrorEventArgs(Exception exception)
        {
            if (exception == null)
            {
                throw new ArgumentNullException(nameof(exception));
            }

            Exception = exception;
        }

        public Exception Exception { get; }
    }
}
