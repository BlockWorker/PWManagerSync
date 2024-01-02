using Newtonsoft.Json;
using System;

namespace PWManagerSync {

    [JsonObject(ItemRequired = Required.Always)]
    public struct IdentEntity {
        [JsonProperty(PropertyName = "ident")] public string Identifier;
        [JsonProperty(PropertyName = "iter")] public uint Iteration;
        [JsonProperty(PropertyName = "symbols")] public string Symbols;
        [JsonProperty(PropertyName = "longpw")] public bool LongPW;
        [JsonProperty(PropertyName = "timestamp")] public DateTime Timestamp;
    }


    [JsonObject(ItemRequired = Required.Always)]
    public struct AppEntity {
        [JsonProperty(PropertyName = "pkg")] public string Package;
        [JsonProperty(PropertyName = "ident")] public string Identifier;
        [JsonProperty(PropertyName = "timestamp")] public DateTime Timestamp;
    }


    [JsonObject(ItemRequired = Required.Always)]
    public struct SyncRequest {
        [JsonProperty(PropertyName = "token")] public string User;
        [JsonProperty(PropertyName = "last_sync")] public DateTime LastSync;
        [JsonProperty(PropertyName = "include_apps")] public bool IncludeApps;
        [JsonProperty(PropertyName = "idents")] public List<IdentEntity> Idents;
        [JsonProperty(PropertyName = "apps")] public List<AppEntity> Apps;
    }


    [JsonObject(ItemRequired = Required.Always)]
    public struct SyncResponse {
        [JsonProperty(PropertyName = "uuid")] public Guid Uuid;
        [JsonProperty(PropertyName = "token")] public string User;
        [JsonProperty(PropertyName = "sync_time")] public DateTime SyncTime;
        [JsonProperty(PropertyName = "changed_idents")] public List<IdentEntity> ChangedIdents;
        [JsonProperty(PropertyName = "changed_apps")] public List<AppEntity> ChangedApps;
        [JsonProperty(PropertyName = "deleted_idents")] public List<string> DeletedIdents;
        [JsonProperty(PropertyName = "deleted_apps")] public List<string> DeletedApps;
    }


    [JsonObject(ItemRequired = Required.Always)]
    public struct SyncConfirmation {
        [JsonProperty(PropertyName = "uuid")] public Guid Uuid;
        [JsonProperty(PropertyName = "token")] public string User;
        [JsonProperty(PropertyName = "sync_time")] public DateTime LastSync;
    }


    public static class Sync {

        private static readonly Dictionary<Guid, string> openRequests = [];

        public static async Task<SyncResponse> ProcessSync(SyncRequest request) {
            var user = request.User;

            //get rid of any prior open requests from this user
            foreach (var existingRequest in openRequests.Where(p => p.Value == user)) {
                openRequests.Remove(existingRequest.Key);
                await Database.Unstage(existingRequest.Key);
            }

            //get last sync time of this user
            var lastSyncServer = (await Database.GetLastSync(user)).GetValueOrDefault(DateTime.UnixEpoch);
            var lastSync = request.LastSync;
            if (lastSync > lastSyncServer) { //client has newer last sync time than server: treat as "never synced before" - rather add too many items than delete too many
                lastSync = DateTime.UnixEpoch;
            }

            //user's complete data on the server
            var serverIdents = await Database.GetAllIdents(user);

            //user's data only present on the server (missing on client)
            var serverOnlyIdents = serverIdents.Where(c => !request.Idents.Any(s => c.Identifier == s.Identifier));

            //user's data only present on the client (missing on server)
            var clientOnlyIdents = request.Idents.Where(c => !serverIdents.Any(s => c.Identifier == s.Identifier));

            //data to be added and deleted on the server
            List<IdentEntity> serverAddIdents = [];
            List<string> serverDeleteIdents = [];
            List<AppEntity> serverAddApps = [];
            List<string> serverDeleteApps = [];

            //data to be added and deleted on the client
            List<IdentEntity> clientAddIdents = [];
            List<string> clientDeleteIdents = [];
            List<AppEntity> clientAddApps = [];
            List<string> clientDeleteApps = [];

            //handle data only present on the server (missing on client): if changed/added after last sync, push to client, otherwise delete on server
            foreach (var ident in serverOnlyIdents) {
                if (ident.Timestamp > lastSync) clientAddIdents.Add(ident);
                else serverDeleteIdents.Add(ident.Identifier);
            }

            //handle data only present on the client (missing on server): if changed/added after last sync, save on server, otherwise delete on client
            foreach (var ident in clientOnlyIdents) {
                if (ident.Timestamp > lastSync) serverAddIdents.Add(ident);
                else clientDeleteIdents.Add(ident.Identifier);
            }

            //handle data present on both client and server: if server newer, push server to client, if client newer, save client on server - do nothing if equal
            foreach (var serverIdent in serverIdents) {
                try {
                    var clientIdent = request.Idents.First(c => serverIdent.Identifier == c.Identifier);
                    
                    var timeDiff = (serverIdent.Timestamp - clientIdent.Timestamp).Ticks;

                    if (Math.Abs(timeDiff) < TimeSpan.TicksPerMillisecond) continue; //treat differences <1ms as "same time" to avoid changes triggered by representation differences
                    else if (timeDiff > 0) clientAddIdents.Add(serverIdent);
                    else serverAddIdents.Add(clientIdent);
                } catch {
                    continue; //not present on client: ignore
                }
            }

            var uuid = Guid.NewGuid();

            //stage sync updates on the server
            await Database.StageInsertIdents(uuid, user, serverAddIdents);
            await Database.StageDeleteIdents(uuid, user, serverDeleteIdents);

            //same process for apps if they are included
            if (request.IncludeApps) {
                var serverApps = await Database.GetAllApps(user);
                var serverOnlyApps = serverApps.Where(c => !request.Apps.Any(s => c.Package == s.Package));
                var clientOnlyApps = request.Apps.Where(c => !serverApps.Any(s => c.Package == s.Package));

                foreach (var app in serverOnlyApps) {
                    if (app.Timestamp > lastSync) clientAddApps.Add(app);
                    else serverDeleteApps.Add(app.Package);
                }

                foreach (var app in clientOnlyApps) {
                    if (app.Timestamp > lastSync) serverAddApps.Add(app);
                    else clientDeleteApps.Add(app.Package);
                }

                foreach (var serverApp in serverApps) {
                    try {
                        var clientApp = request.Apps.First(c => serverApp.Package == c.Package);

                        var timeDiff = (serverApp.Timestamp - clientApp.Timestamp).Ticks;

                        if (Math.Abs(timeDiff) < TimeSpan.TicksPerMillisecond) continue; //same as above
                        else if (timeDiff > 0) clientAddApps.Add(serverApp);
                        else serverAddApps.Add(clientApp);
                    } catch {
                        continue; //not present on client: ignore
                    }
                }

                await Database.StageInsertApps(uuid, user, serverAddApps);
                await Database.StageDeleteApps(uuid, user, serverDeleteApps);
            }

            openRequests.Add(uuid, user);

            Console.WriteLine($"SYNC: Staged {uuid} from user {user}, apps {request.IncludeApps}");
            Console.WriteLine($"  - Server: Add/change {serverAddIdents.Count} idents and {serverAddApps.Count} apps; Delete {serverDeleteIdents.Count} idents and {serverDeleteApps.Count} apps");
            Console.WriteLine($"  - Client: Add/change {clientAddIdents.Count} idents and {clientAddApps.Count} apps; Delete {clientDeleteIdents.Count} idents and {clientDeleteApps.Count} apps");

            return new SyncResponse {
                Uuid = uuid,
                User = user,
                SyncTime = DateTime.UtcNow,
                ChangedIdents = [.. clientAddIdents],
                ChangedApps = [.. clientAddApps],
                DeletedIdents = [.. clientDeleteIdents],
                DeletedApps = [.. clientDeleteApps],
            };
        }

        public static async Task<bool> CommitSync(SyncConfirmation confirmation) {
            var uuid = confirmation.Uuid;

            if (openRequests.TryGetValue(uuid, out var user)) { //find associated sync request
                if (user != confirmation.User) return false; //if found but associated with another user, reject

                await Database.CommitStaged(uuid);
                await Database.SetLastSync(user, confirmation.LastSync);
                openRequests.Remove(uuid);

                Console.WriteLine($"CONFIRM: Committed {uuid} from user {user} to database");

                return true;
            } else { //no associated sync request: reject, and delete anything staged for the given uuid (just in case there are some random leftovers)
                await Database.Unstage(uuid);
                return false;
            }
        }
    }

}
