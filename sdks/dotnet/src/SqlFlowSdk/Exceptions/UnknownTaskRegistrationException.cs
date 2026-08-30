using System;
using System.Collections.Generic;
using System.Text;

namespace SqlFlowSdk.Exceptions;

public sealed class UnknownTaskRegistrationException : Exception
{
    public UnknownTaskRegistrationException(string message)
        : base(message)
    {
    }
}