using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;

namespace Rbkl.io
{
    internal class MyTelemetryProcessor : ITelemetryProcessor
    {
        private readonly ITelemetryProcessor Next;

        public MyTelemetryProcessor(ITelemetryProcessor next)
        {
            this.Next = next;
        }
        public void Process(ITelemetry item)
        {
            if (item is DependencyTelemetry depedency)
            {
                //Filter 409 depedency result codes
                if (depedency.ResultCode == "409")
                {
                    return;
                }
            }
            this.Next.Process(item);
        }
    }
}