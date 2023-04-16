using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;

namespace Meta {
    namespace Marketing {
        public class AccessToken {
            public string Token = "";
            public int ExpiresIn;
        }

        public static class AccessTokenHelper {
            public static async Task< AccessToken > GetNewAccessToken( string appId, string appSecret, string currentToken ) {
                string query = $"https://graph.facebook.com/oauth/access_token?client_id={appId}&client_secret={appSecret}&grant_type=fb_exchange_token&fb_exchange_token={currentToken}";

                var httpClient = new HttpClient( );

                var response = await httpClient.GetAsync( query );

                response.EnsureSuccessStatusCode( );

                var jsonData = await response.Content.ReadAsStringAsync( );

                var data = JsonConvert.DeserializeObject< dynamic >( jsonData );

                return new AccessToken {
                    Token = data.access_token,
                    ExpiresIn = data.expires_in,
                };
            }
        }
    }

}
