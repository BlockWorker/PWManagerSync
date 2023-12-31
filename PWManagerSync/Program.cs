using Ceen;
using Ceen.Httpd;
using Ceen.Httpd.Handler;
using Ceen.Httpd.Logging;
using Newtonsoft.Json;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace PWManagerSync {
    public static class Program {

        public const int VERSION = 1;
        public const string CONFIG_PATH = "pwmsync.config";
        private static string sslCertPath = "";
        private static string sslCertPass = "";
        private static int serverPort = 1623;

        private static readonly CancellationTokenSource tcs = new();

        private static void LoadConfig() {
            if (!File.Exists(CONFIG_PATH)) throw new FileNotFoundException("Config file must be present.");
            var lines = File.ReadAllLines(CONFIG_PATH);
            foreach (var line in lines) {
                if (line.StartsWith('#')) continue;
                var parts = line.Split('=', 2);
                switch (parts[0]) {
                    case "CertPath":
                        sslCertPath = parts[1];
                        break;
                    case "CertPass":
                        sslCertPass = parts[1];
                        break;
                    case "Port":
                        serverPort = int.Parse(parts[1]);
                        break;
                }
            }
        }

        private static void HandleIntTermSignal(PosixSignalContext context) {
            Console.WriteLine($"Received {context.Signal}, exiting...");
            context.Cancel = true;
            tcs.Cancel();
        }

        public static async Task Main(string[] args) {
            PosixSignalRegistration.Create(PosixSignal.SIGINT, HandleIntTermSignal);
            PosixSignalRegistration.Create(PosixSignal.SIGTERM, HandleIntTermSignal);

            LoadConfig();
            if (!await Database.Init()) return; //don't even try to start unless we have a working database

            var config = new ServerConfig()
                .AddLogger(new CLFStdOut()) //log to stdout
                .AddRoute("/ping", new PingHandler()) //handle ping requests
                .AddRoute("[^/(sync|confirm)$]", new SyncHandler()) //handle sync and confirm requests
                .AddRoute(new StaticHandler()); //respond to all other requests with 404

            config.SSLCertificate = new X509Certificate2(sslCertPath, sslCertPass);

            var task = HttpServer.ListenAsync( //start server to listen on all interfaces, with the above config, and SSL enabled
                new System.Net.IPEndPoint(System.Net.IPAddress.Any, serverPort),
                true,
                config,
                tcs.Token
            );

            Console.WriteLine("Server started.");

            await task; //wait for server to stop (triggered externally)

            Database.Close();

            Console.WriteLine("Server stopped.");
        }

        private class PingHandler : IHttpModule {
            public async Task<bool> HandleAsync(IHttpContext context) {
                var req = context.Request;
                var res = context.Response;

                if (!string.Equals(req.Method, "GET", StringComparison.Ordinal)) { //only support GET method
                    throw new HttpException(HttpStatusCode.MethodNotAllowed);
                }

                res.SetNonCacheable();

                await res.WriteAllJsonAsync("""{"pwm_sync_version": """ + VERSION + "}"); //simple JSON response with version

                return true;
            }
        }


        private class SyncHandler : IHttpModule {
            public async Task<bool> HandleAsync(IHttpContext context) {
                var req = context.Request;
                var res = context.Response;

                var path = req.Path ?? "/";

                if (!string.Equals(req.Method, "POST", StringComparison.Ordinal)) { //only support POST method
                    throw new HttpException(HttpStatusCode.MethodNotAllowed);
                }

                var type = req.ContentType;
                if (type == null || !type.Contains("application/json", StringComparison.OrdinalIgnoreCase)) { //only support JSON content
                    throw new HttpException(HttpStatusCode.UnsupportedMediaType);
                }

                res.SetNonCacheable();

                var encoding = req.GetEncodingForContentType() ?? Encoding.UTF8;
                var content = await req.Body.ReadAllAsStringAsync(encoding) ?? "";

                var serSettings = new JsonSerializerSettings {
                    MissingMemberHandling = MissingMemberHandling.Error, //be strict about json members
                    DateTimeZoneHandling = DateTimeZoneHandling.Utc //all our times are always in UTC
                };

                try {
                    if (path.Equals("/sync", StringComparison.Ordinal)) { //sync request
                        var sReq = JsonConvert.DeserializeObject<SyncRequest>(content, serSettings);

                        try {
                            var resp = await Sync.ProcessSync(sReq); //process actual sync and stage it

                            await res.WriteAllJsonAsync(JsonConvert.SerializeObject(resp)); //send sync response back
                        } catch (Exception ex) {
                            Console.WriteLine(ex.ToString());
                            throw new HttpException(HttpStatusCode.InternalServerError);
                        }
                    } else if (path.Equals("/confirm", StringComparison.Ordinal)) { //confirm request
                        var sConf = JsonConvert.DeserializeObject<SyncConfirmation>(content, serSettings);

                        var requestFound = false;
                        try {
                            requestFound = await Sync.CommitSync(sConf); //try committing confirmed sync
                        } catch (Exception ex) {
                            Console.WriteLine(ex.ToString());
                            throw new HttpException(HttpStatusCode.InternalServerError);
                        }

                        if (!requestFound) { //if confirmed sync not found: return error
                            throw new HttpException(HttpStatusCode.NotFound, "Sync Request Not Found");
                        }
                    } else { //unknown request (should not happen)
                        throw new HttpException(HttpStatusCode.NotFound);
                    }
                } catch (JsonException) { //return error if json parsing fails
                    throw new HttpException(HttpStatusCode.BadRequest, "Bad Request: Invalid JSON format");
                }

                return true;
            }
        }
    }
}
