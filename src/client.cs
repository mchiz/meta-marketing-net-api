using Microsoft.CSharp.RuntimeBinder;
using Newtonsoft.Json;
using System;
using System.ComponentModel;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading;
using System.Xml.Linq;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Meta {
    namespace Marketing {
        public enum CustomAudienceSource {
            UserProvidedOnly,
            PartnerProvidedOnly,
            BothUserAndPartnerProvided,
        }

        public struct AudienceInfo {
            [ JsonProperty( "id" ) ] public string Id;
            [ JsonProperty( "name" ) ] public string Name;
            [ JsonProperty( "description" ) ] public string Description;
        }

        public struct Insights {
            public double Spend;
            public ulong Impressions;
            public ulong Reach;

            public ulong OutboundClicks;
            public ulong LinkClick;
            public ulong LeadsCount;
            public ulong ConversionsCount;
            public ulong PostReaction;

        }

        public class Client : IDisposable {
            public Client( string accountId, string accessToken ) {
                _accountId = accountId;
                AccessToken = accessToken;
            }

            public async Task RetreiveLeadFormLeads( string leadFormId, CancellationToken cancellationToken = default ) {
                var long_lived_page_access_token = "EAAj93xsZC3yEBAEVVg3QlVqHqITnDW6vE2Obc8AZBtI9Wh0rIoDzms8jbJXVxMM4Vzu9ZBmZCslBLmkn2Ydte7eGEdXhQdXPMSZADSuswitTLV29Rn1EczZCMu63LTal9RkwsSJlIQHjWXlDWD9xOifuhTKmCYZCWaVYkIYkfGDpkyOcrftF5NrJoviy20GPj3yS9ErG2E7MgZDZD";

                var lead_data_endpoint = $"https://graph.facebook.com/v20.0/{leadFormId}/leads?access_token=" + long_lived_page_access_token;
                var lead_data_response = await _httpClient.GetAsync( lead_data_endpoint, cancellationToken );
                var lead_raw_data = Newtonsoft.Json.JsonConvert.DeserializeObject< dynamic >( await lead_data_response.Content.ReadAsStringAsync( cancellationToken ) );
                var lead_data = lead_raw_data.data;

                var initial = true;
                var to_proceed = true;
                var subsequent_api_endpoint = "";

                while( to_proceed ) {
                  // Trigger another Graph API call to paginate to the next page
                  if ( !initial && !string.IsNullOrEmpty( subsequent_api_endpoint ) ) {
                    lead_data_response = await _httpClient.GetAsync( subsequent_api_endpoint, cancellationToken );
                    lead_raw_data = Newtonsoft.Json.JsonConvert.DeserializeObject< dynamic >( await lead_data_response.Content.ReadAsStringAsync( cancellationToken ) );
                    lead_data = lead_raw_data.data;
                  }

                  // Process all of the leads in this page (one page contains 25 leads)
                  for( var z = 0; z < lead_data.length; z++ ) {
                    // Get current lead information
                    var current_lead = lead_data[z];
                    var field_data = current_lead.field_data;
                    var final_lead_information = new Dictionary< string, string >( );

                    // Compile all data in an object
                    for( var x = 0; x < field_data.length; x++ ) {
                      final_lead_information[field_data[x].name] = field_data[x].values[0];
                    }

                    // Get the date and time we received the lead
                    //var d = new Date( current_lead.created_time );
                    //var submitted_at = d.getFullYear() + '-' + (d.getMonth() + 1) + '-' + d.getDate() + ' ' + d.getHours() + ':' + d.getMinutes() + ':' + d.getSeconds();
                    //final_lead_information['submitted_at'] = submitted_at;

                    // Arrange them in the proper order
                    var final_order = new List< string >( );
                    var format_order = new string[ ] {"full_name", "phone_number", "email", "company_name", "job_title", "submitted_at" } ;
                    for( var y = 0; y < format_order.Length; y++ ) {
                       final_order.Add(final_lead_information[format_order[y]]);
                    }

                    // Record it in the Google Sheets
                    //active_sheet.appendRow(final_order);

                  }

                  subsequent_api_endpoint = lead_raw_data["paging"]["next"] != null ? lead_raw_data["paging"]["next"] : "";
                  to_proceed = lead_raw_data["paging"]["next"] != null;

                  initial = false;
                }
            }

            public async Task< Insights > RetreiveInsights( DateTime from, DateTime to, string filter, string [ ]fields, CancellationToken cancellationToken = default ) {
                string query = $"https://graph.facebook.com/v20.0/{filter}/insights?";

                query += $"access_token={AccessToken}";
  
                query += $"&time_range[since]=" + from.Year + "-" + from.Month + "-" + from.Day;
                query += $"&time_range[until]=" + to.Year + "-" + to.Month + "-" + to.Day;

                if( fields.Length > 0 ) {
                    query += "&fields=" + fields[ 0 ];
    
                    for( var i = 1; i < fields.Length; ++i )
                      query += "," + fields[ i ];
                }

                using var response = await _httpClient.GetAsync( query, cancellationToken );

                response.EnsureSuccessStatusCode( );

                string responseJsonData = await response.Content.ReadAsStringAsync( cancellationToken );

                var result = new Insights( );

                var data = Newtonsoft.Json.JsonConvert.DeserializeObject< InsightsResponse >( responseJsonData );
                
                if( data.Data.Length == 0 )
                    return result;

                var d = data.Data[ 0 ];
                
                result.Spend = d.Spend;
                result.Impressions = d.Impressions;
                result.Reach = d.Reach;

                if( d.Actions != null ) {
                    for( int i = 0; i < d.Actions.Length; ++i ) {
                        var a = d.Actions[ i ];

                        switch( a.ActionType ) {
                            case "link_click": result.LinkClick = a.Value; break;
                            case "lead": result.LeadsCount = a.Value; break;
                            case "post_reaction": result.PostReaction = a.Value; break;
                            default:
                                if( a.ActionType.Contains( "onsite_conversion" )
                                 || a.ActionType.Contains( "offsite_conversion" ) )
                                    result.ConversionsCount += a.Value;
                                break;
                        }
                    }
                }

                if( d.OutboundClicks != null ) {
                    for( int i = 0; i < d.OutboundClicks.Length; ++i ) {
                        var a = d.OutboundClicks[ i ];

                        switch( a.ActionType ) {
                            case "outbound_click": result.OutboundClicks = a.Value; break;
                        }
                    }
                }

                return result;
            }

            public async Task< AudienceInfo[ ] > EnumerateCustomAudiences( CancellationToken cancellationToken = default ) {
                string url = $"https://graph.facebook.com/v20.0/act_{_accountId}/customaudiences?fields=id,name,description&access_token={AccessToken}";

                using var response = await _httpClient.GetAsync( url, cancellationToken );

                response.EnsureSuccessStatusCode( );

                string responseJsonData = await response.Content.ReadAsStringAsync( cancellationToken );

                var responseData = Newtonsoft.Json.JsonConvert.DeserializeObject< EnumerateCustomAudiencesResponse >( responseJsonData );

                return responseData.Data;
            }

            public async Task< AudienceInfo > CreateCustomAudience( string name, string description, CustomAudienceSource cas, CancellationToken cancellationToken = default ) {
                string url = $"https://graph.facebook.com/v20.0/act_{_accountId}/customaudiences";

                string cfs = "";

                switch( cas ) {
                    case CustomAudienceSource.UserProvidedOnly: cfs = "USER_PROVIDED_ONLY"; break;
                    case CustomAudienceSource.PartnerProvidedOnly: cfs = "PARTNER_PROVIDED_ONLY"; break;
                    case CustomAudienceSource.BothUserAndPartnerProvided: cfs = "BOTH_USER_AND_PARTNER_PROVIDED"; break;
                }

                var content = new FormUrlEncodedContent( new Dictionary< string, string > {
                    { "name", name },
                    { "subtype", "CUSTOM" },
                    { "description", description },
                    { "customer_file_source", cfs },
                    { "access_token", AccessToken },
                } );

                using var response = await _httpClient.PostAsync( url, content, cancellationToken );
                
                response.EnsureSuccessStatusCode( );

                var jsonResponse = await response.Content.ReadAsStringAsync( cancellationToken );

                var result = Newtonsoft.Json.JsonConvert.DeserializeObject< CustomAudienceCreatedResponse >( jsonResponse );

                return new AudienceInfo {
                    Id = result.Id,
                    Name = name,
                    Description = description,
                };
            }

            public async Task AddUsersToCustomAudience( string customAudienceId, UserData [ ]users, CancellationToken cancellationToken = default ) {
                if( users.Length == 0 )
                    return;

                bool hasFirstName = !string.IsNullOrWhiteSpace( users[ 0 ].FirstName );

                foreach( var ud in users ) {
                    bool has = !string.IsNullOrWhiteSpace( ud.FirstName );

                    if( hasFirstName != has )
                        throw new Exception( "Inconsistence data provided in the FirstName users field" );
                }

                bool hasLastName = !string.IsNullOrWhiteSpace( users[ 0 ].LastName );

                foreach( var ud in users ) {
                    bool has = !string.IsNullOrWhiteSpace( ud.LastName );

                    if( hasLastName != has )
                        throw new Exception( "Inconsistence data provided in the LastName users field" );
                }

                bool hasEmail = !string.IsNullOrWhiteSpace( users[ 0 ].Email );

                foreach( var ud in users ) {
                    bool has = !string.IsNullOrWhiteSpace( ud.Email );

                    if( hasEmail != has )
                        throw new Exception( "Inconsistence data provided in the Email users field" );
                }

                var schemas = new List< string >( );

                if( hasFirstName ) schemas.Add( "FN" );
                if( hasLastName ) schemas.Add( "LN" );
                if( hasEmail ) schemas.Add( "EMAIL" );

                if( schemas.Count == 0 )
                    throw new Exception( "UserData provided is absolutely empty" );

                var data = new List< List< string > >( );

                for( int i = 0; i < users.Length; ++i ) {
                    var ud = users[ i ];

                    var d = new List< string >( );

                    if( hasFirstName ) d.Add( SHA256Hash( ud.FirstName ) );
                    if( hasLastName ) d.Add( SHA256Hash( ud.LastName ) );
                    if( hasEmail ) d.Add( SHA256Hash( ud.Email ) );

                    data.Add( d );
                }

                if( users.Length <= _maxUsersPerCustomAudienceBatch ) {
                    var payload = new Dictionary< string, object >( );

                    payload[ "schema" ] = schemas;
                    payload[ "data" ] = data;
                
                    string url = $"https://graph.facebook.com/v20.0/{customAudienceId}/users";

                    var content = new FormUrlEncodedContent( new Dictionary< string, string >( ) { 
                        { "payload", Newtonsoft.Json.JsonConvert.SerializeObject( payload ) },
                        { "access_token", AccessToken },
                    } );

                    using var result = await _httpClient.PostAsync( url, content, cancellationToken );

                    result.EnsureSuccessStatusCode( );

                    string responseJsonData = await result.Content.ReadAsStringAsync( cancellationToken );
                
                    var response = Newtonsoft.Json.JsonConvert.DeserializeObject< AddUsersToCustomAudienceResponse >( responseJsonData );

                } else {
                    int remainingUsersCount = data.Count;
                    int currentIndex = 0;
                    int currentBatchId = 1;

                    var rnd = new Random( );
                    int sessionId = rnd.Next( );

                    while( remainingUsersCount > 0 ) {
                        bool lastBatch = remainingUsersCount < _maxUsersPerCustomAudienceBatch;

                        int thisChunkSize = remainingUsersCount;
                        if( thisChunkSize > _maxUsersPerCustomAudienceBatch )
                            thisChunkSize = _maxUsersPerCustomAudienceBatch;

                        var chunkData = new List< List< string > >( thisChunkSize );

                        for( int j = 0; j < thisChunkSize; ++j )
                            chunkData.Add( data[ currentIndex + j ] );

                        currentIndex += thisChunkSize;
                        remainingUsersCount -= thisChunkSize;

                        var payload = new Dictionary< string, object >( );

                        payload[ "schema" ] = schemas;
                        payload[ "data" ] = chunkData;
                
                        var session = new {
                            session_id = sessionId,
                            batch_seq = currentBatchId,
                            last_batch_flag = lastBatch,
                            estimated_num_total = users.Length,
                        };

                        ++currentBatchId;

                        string url = $"https://graph.facebook.com/v20.0/{customAudienceId}/users";

                        var content = new FormUrlEncodedContent( new Dictionary< string, string >( ) { 
                            { "payload", Newtonsoft.Json.JsonConvert.SerializeObject( payload ) },
                            { "access_token", AccessToken },
                            { "session", Newtonsoft.Json.JsonConvert.SerializeObject( session ) },
                        } );

                        using var result = await _httpClient.PostAsync( url, content, cancellationToken );
                        
                        result.EnsureSuccessStatusCode( );

                        string responseJsonData = await result.Content.ReadAsStringAsync( cancellationToken );
                
                        var response = Newtonsoft.Json.JsonConvert.DeserializeObject< AddUsersToCustomAudienceResponse >( responseJsonData );

                    }

                }

            }

            void IDisposable.Dispose( ) {
                _httpClient.Dispose( );
            }

            static string SHA256Hash( string value ) {
                var sb = new StringBuilder( );

                using var hash = System.Security.Cryptography.SHA256.Create( );
    
                Encoding enc = Encoding.UTF8;
                var result = hash.ComputeHash( enc.GetBytes( value ) );

                foreach( var b in result )
                    sb.Append( b.ToString( "x2" ) );

                return sb.ToString( );
            }


            struct CustomAudienceCreatedResponse {
                [ JsonProperty( "id" ) ] public string Id;
            }

            //{"audience_id":"23853262716910525","session_id":"67972159610631945","num_received":1,"num_invalid_entries":0,"invalid_entry_samples":{}}
            struct AddUsersToCustomAudienceResponse {
                [ JsonProperty( "audience_id" ) ] public string AudienceId;
                [ JsonProperty( "session_id" ) ] public string SessionId;
                [ JsonProperty( "num_received" ) ] public int NumReceived;
                [ JsonProperty( "num_invalid_entries" ) ] public int NumInvalidEntries;
            }

            struct EnumerateCustomAudiencesResponse {
                [ JsonProperty( "data" ) ] public AudienceInfo [ ]Data;
            }

            struct InsightsResponse {
                public struct Response {
                    public struct Action {
                        [ JsonProperty( "action_type" ) ] public string ActionType;
                        [ JsonProperty( "value" ) ] public ulong Value;
                    }

                    [ JsonProperty( "spend", DefaultValueHandling = DefaultValueHandling.Populate ) ] public double Spend;
                    [ JsonProperty( "impressions", DefaultValueHandling = DefaultValueHandling.Populate ) ] public ulong Impressions;
                    [ JsonProperty( "reach", DefaultValueHandling = DefaultValueHandling.Populate ) ] public ulong Reach;
                    [ JsonProperty( "actions" ) ] public Action [ ]Actions;
                    [ JsonProperty( "outbound_clicks" ) ] public Action [ ]OutboundClicks;

                }

                [ JsonProperty( "data" ) ] public Response [ ]Data;
            }

            string _accountId;
            public string AccessToken;

            HttpClient _httpClient = new HttpClient( );

            const int _maxUsersPerCustomAudienceBatch = 10000;
        }
    }
}