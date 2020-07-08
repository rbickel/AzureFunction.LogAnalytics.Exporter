using System.Linq;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Hosting;
using Microsoft.Extensions.DependencyInjection;

// [assembly: WebJobsStartup(typeof(Rbkl.io.MyStartup))]
namespace Rbkl.io
{
    public class MyStartup : IWebJobsStartup
    {
        public void Configure(IWebJobsBuilder builder)
        {
            var configDescriptor = builder.Services.SingleOrDefault(tc => tc.ServiceType == typeof(TelemetryConfiguration));
            if (configDescriptor?.ImplementationFactory == null)
                return;

            var implFactory = configDescriptor.ImplementationFactory;

            builder.Services.Remove(configDescriptor);
            builder.Services.AddSingleton(provider =>
            {
                if (!(implFactory.Invoke(provider) is TelemetryConfiguration config))
                    return null;

                config.TelemetryProcessorChainBuilder.Use(next => new MyTelemetryProcessor(next));
                config.TelemetryProcessorChainBuilder.Build();

                return config;
            });
        }
    }
}