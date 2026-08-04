// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Exceptions;

public class SuspendTaskException : Exception
{
    public SuspendTaskException() : base("Task suspended") { }
}
