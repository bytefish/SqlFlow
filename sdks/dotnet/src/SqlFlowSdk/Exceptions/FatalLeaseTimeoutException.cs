// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Exceptions;

public sealed class FatalLeaseTimeoutException : Exception
{
    public FatalLeaseTimeoutException(string message) : base(message)
    {
    }

    public FatalLeaseTimeoutException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}