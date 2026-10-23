using System;

namespace Sx.Communication
{
    public sealed class TcpStateChangedEventArgs : EventArgs
    {
        public TcpStateChangedEventArgs(TcpClientState state, TcpClientState previousState)
        {
            State = state;
            PreviousState = previousState;
        }

        public TcpClientState State { get; }

        public TcpClientState PreviousState { get; }
    }
}
