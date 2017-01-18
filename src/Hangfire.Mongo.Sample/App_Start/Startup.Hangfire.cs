using Owin;
using System;

namespace Hangfire.Mongo.Sample
{
    public partial class Startup
    {
        public void ConfigureHangfire(IAppBuilder app)
        {
            GlobalConfiguration.Configuration.UseMongoStorage("mongodb://localhost", "hangfire-mongo-sample", new MongoStorageOptions() { QueuePollInterval = TimeSpan.FromSeconds(1) });
            //GlobalConfiguration.Configuration.UseMongoStorage(new MongoClientSettings()
            //{
            //    // ...
            //    IPv6 = true
            //}, "hangfire-mongo-sample");

            app.UseHangfireServer();
            app.UseHangfireDashboard();
        }
    }
}
