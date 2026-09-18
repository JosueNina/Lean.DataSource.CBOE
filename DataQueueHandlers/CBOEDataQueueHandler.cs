/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.IO;
using System.Net;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using System.Runtime.InteropServices;
using System.Net.NetworkInformation;
using System.Security.Cryptography;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QuantConnect.Api;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Securities.Index;
using QuantConnect.Util;
using Config = QuantConnect.Configuration.Config;
using Library = Confluent.Kafka.Library;
using DateTimeZone = NodaTime.DateTimeZone;

namespace QuantConnect.DataSource.DataQueueHandlers
{
    /// <summary>
    /// Cboe Global Cloud <see cref="IDataQueueHandler"/> implementation, streams the Cboe Global Indices feed.
    /// The feed is served by a Kafka cluster hosted in AWS, authenticated using mutual TLS.
    /// </summary>
    /// <remarks>
    /// Feed specification: https://www.cboe.com/document/tech-spec/content/technical-specifications/cboe-titanium-cboe-global-cloud-feed-specification
    /// Setup guide: https://www.cboe.com/document/tech-spec/content/technical-specifications/cboe-titanium-cboe-global-cloud-setup-guide
    /// </remarks>
    public class CBOEDataQueueHandler : IDataQueueHandler
    {
        private const string PemHeader = "-----BEGIN";
        private static readonly Regex _indexTopicRegex = new("^[a-z0-9]+-idx-[a-z0-9]+$", RegexOptions.Compiled);

        /// <summary>
        /// The value block type holding the index value, the rest being bid (0), ask (1), close (4) and settlement (5)
        /// </summary>
        private const int IndexValueType = 3;

        private static bool _nativeLibraryResolverSet;
        private static readonly TimeSpan _metadataTimeout = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan _retryDelay = TimeSpan.FromSeconds(10);

        private readonly IDataAggregator _dataAggregator;
        // the indexes seen in the feed by ticker, only used by the consumer thread. Null for those we don't serve
        private readonly Dictionary<string, IndexDetails> _indexes = new();
        private readonly CancellationTokenSource _cancellationTokenSource = new();

        private MarketHoursDatabase _marketHoursDatabase;
        private Thread _consumerThread;
        private volatile bool _connected;
        private bool _initialized;

        /// <summary>
        /// Returns whether the data provider is connected
        /// </summary>
        /// <returns>True if the data provider is connected</returns>
        public bool IsConnected => _connected;

        /// <summary>
        /// Creates a new instance, the connection will be started once the job is set, see <see cref="SetJob"/>
        /// </summary>
        public CBOEDataQueueHandler()
        {
            _dataAggregator = Composer.Instance.GetPart<IDataAggregator>();
            if (_dataAggregator == null)
            {
                var aggregatorName = Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager");
                Log.Trace($"CBOEDataQueueHandler(): found no data aggregator instance, creating {aggregatorName}");
                _dataAggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(aggregatorName, forceTypeNameOnExisting: false);
            }
        }

        /// <summary>
        /// Sets the job we're subscribing for, starts the connection
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
            if (_initialized)
            {
                return;
            }

            // local deployments will not have these in the job, which might not even have brokerage data, so we fallback to the configuration
            var brokerageData = job?.BrokerageData;
            Initialize(key => brokerageData != null && brokerageData.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : Config.Get(key));
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <remarks>There is nothing to request to the feed, all the data it provides is sent to the aggregator,
        /// which takes care of dropping what there is no subscription for</remarks>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            if (!CanSubscribe(dataConfig))
            {
                return null;
            }

            return _dataAggregator.Add(dataConfig, newDataAvailableHandler);
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _dataAggregator.Remove(dataConfig);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            _consumerThread?.Join(TimeSpan.FromSeconds(10));
            _dataAggregator.DisposeSafely();
        }

        /// <summary>
        /// Handles a Cboe Global Indices feed message, emitting the index value to the aggregator
        /// </summary>
        /// <param name="message">The json message</param>
        /// <param name="topic">The topic the message is from</param>
        protected void ProcessMessage(string message, string topic = null)
        {
            try
            {
                if (Log.DebuggingEnabled)
                {
                    Log.Debug($"CBOEDataQueueHandler.ProcessMessage(): {message}");
                }

                // the spec defines numeric fields as strings of digits but the examples mix both representations, json.net handles both
                var indexMessage = JsonConvert.DeserializeObject<CBOEIndexMessage>(message);

                // index value updates, with and without status. Decoders should ignore unknown message types
                if (indexMessage?.Values == null
                    || indexMessage.MessageType != "X" && indexMessage.MessageType != "I"
                    || string.IsNullOrEmpty(indexMessage.Symbol))
                {
                    return;
                }

                var index = GetIndex(indexMessage.Symbol, topic);
                if (index == null)
                {
                    return;
                }

                foreach (var value in indexMessage.Values)
                {
                    if (value.Type == IndexValueType)
                    {
                        var time = Time.UnixMillisecondTimeStampToDateTime(indexMessage.SendTime).ConvertFromUtc(index.ExchangeTimeZone);
                        _dataAggregator.Update(new Tick(time, index.Symbol, string.Empty, string.Empty, 0, value.Value));
                    }
                }
            }
            catch (Exception exception)
            {
                Log.Error(exception, $"Failed to process message: {message}");
            }
        }

        /// <summary>
        /// Gets the exchange time zone of the given symbol
        /// </summary>
        protected virtual DateTimeZone GetExchangeTimeZone(Symbol symbol)
        {
            return _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType).TimeZone;
        }

        /// <summary>
        /// Gets the lean symbol and exchange time zone for the given ticker, null if not served
        /// </summary>
        private IndexDetails GetIndex(string ticker, string topic)
        {
            if (!_indexes.TryGetValue(ticker, out var index))
            {
                // international indices have their own lean market and are not served here, see 'CanSubscribe'
                if (!IndexSymbol.TryGetIndexMarket(ticker, out _))
                {
                    var symbol = Symbol.Create(ticker, SecurityType.Index, Market.USA);
                    index = new IndexDetails(symbol, GetExchangeTimeZone(symbol));
                }
                _indexes[ticker] = index;
                Log.Trace($"CBOEDataQueueHandler.GetIndex(): new ticker '{ticker}' topic '{topic}'{(index == null ? ", not served" : $" time zone {index.ExchangeTimeZone}")}. Count: {_indexes.Count}");
            }
            return index;
        }

        /// <summary>
        /// Starts the connection using the given configuration source
        /// </summary>
        private void Initialize(Func<string, string> getConfig)
        {
            string GetRequired(string key)
            {
                var value = getConfig(key);
                if (string.IsNullOrWhiteSpace(value))
                {
                    throw new ArgumentException($"CBOEDataQueueHandler: '{key}' is a required configuration value");
                }
                return value;
            }

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = GetRequired("cboe-bootstrap-servers"),
                // the case sensitive group id assigned by Cboe
                GroupId = GetRequired("cboe-group-id"),
                SecurityProtocol = SecurityProtocol.Ssl,
                SslKeyPassword = getConfig("cboe-ssl-key-password"),
                SocketKeepaliveEnable = true,
                // we always start from the latest value and never commit, see 'GetPartitions'
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            // these can be a file path or the PEM content itself, useful when there's no file system access to set them up
            SetPemOrLocation(GetRequired("cboe-ssl-certificate"), pem => consumerConfig.SslCertificatePem = pem, path => consumerConfig.SslCertificateLocation = path);
            SetPemOrLocation(GetRequired("cboe-ssl-key"), pem => consumerConfig.SslKeyPem = pem, path => consumerConfig.SslKeyLocation = path);
            // optional, by default will use the system trusted root certificates
            SetPemOrLocation(getConfig("cboe-ssl-ca"), pem => consumerConfig.SslCaPem = pem, path => consumerConfig.SslCaLocation = path);

            // optional, by default we consume all the index topics we have access to
            var topicList = (getConfig("cboe-topics") ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            // once we know the configuration is there
            ValidateSubscription();

            _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();

            // optional, file or directory to load the native kafka client from, by default we will look for it
            LoadNativeLibrary(getConfig("cboe-librdkafka-location"));

            _initialized = true;
            _consumerThread = new Thread(() => ConsumeLoop(consumerConfig, topicList)) { IsBackground = true, Name = "CBOEDataQueueHandler" };
            _consumerThread.Start();
        }

        /// <summary>
        /// Consumes the feed until disposed. The kafka client will handle reconnections internally,
        /// if anything else goes wrong we start over with a new consumer
        /// </summary>
        private void ConsumeLoop(ConsumerConfig consumerConfig, string[] topics)
        {
            var cancellationToken = _cancellationTokenSource.Token;
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                        .SetErrorHandler(OnError)
                        .SetLogHandler(OnLog)
                        .Build();

                    var partitions = GetPartitions(consumer, topics);
                    consumer.Assign(partitions);
                    _connected = true;
                    Log.Trace($"CBOEDataQueueHandler.ConsumeLoop(): connected, consuming topics: {string.Join(",", partitions.Select(x => x.Topic).Distinct())}");

                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            var result = consumer.Consume(cancellationToken);
                            if (result?.Message?.Value != null)
                            {
                                _connected = true;
                                ProcessMessage(result.Message.Value, result.Topic);
                            }
                        }
                        catch (ConsumeException exception) when (!exception.Error.IsFatal)
                        {
                            // the client will recover by itself
                            Log.Error($"CBOEDataQueueHandler.ConsumeLoop(): {exception.Error.Code} - {exception.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // shutting down
                }
                catch (Exception exception)
                {
                    Log.Error(exception, $"Will retry in {_retryDelay.TotalSeconds}s");
                    _connected = false;
                    cancellationToken.WaitHandle.WaitOne(_retryDelay);
                }
            }
            _connected = false;
            Log.Trace("CBOEDataQueueHandler.ConsumeLoop(): ended");
        }

        /// <summary>
        /// Loads the native kafka client. By default it's found through the application dependencies file, which
        /// will not hold it when we are loaded as a plugin, so we look for it ourselves, in the 'runtimes' folder it ships in or right next to the assemblies
        /// </summary>
        /// <param name="location">Optional file or directory to load it from</param>
        protected static void LoadNativeLibrary(string location = null)
        {
            try
            {
                if (Library.IsLoaded)
                {
                    return;
                }

                var os = OperatingSystem.IsWindows() ? "win" : OperatingSystem.IsMacOS() ? "osx" : "linux";
                var runtime = Path.Combine("runtimes", $"{os}-{RuntimeInformation.ProcessArchitecture.ToString().ToLowerInvariant()}", "native");
                var directories = new[]
                    {
                        Path.GetDirectoryName(typeof(Library).Assembly.Location),
                        Path.GetDirectoryName(typeof(CBOEDataQueueHandler).Assembly.Location),
                        AppContext.BaseDirectory,
                        Directory.GetCurrentDirectory()
                    }
                    .Where(x => !string.IsNullOrEmpty(x))
                    .SelectMany(x => new[] { Path.Combine(x, runtime), x })
                    // the given location goes first
                    .Prepend(location)
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .Select(Path.GetFullPath)
                    .Distinct()
                    .ToList();

                // The kafka client own explicit load, 'Library.Load(path)', requires 'libdl.so' which is not available in recent linux distributions,
                // so we resolve the libraries it asks for instead: the default build 'librdkafka' first, then the alternative ones, e.g. 'centos8-librdkafka'
                if (!_nativeLibraryResolverSet)
                {
                    _nativeLibraryResolverSet = true;
                    // we get called for each native method
                    var handles = new ConcurrentDictionary<string, IntPtr>();
                    NativeLibrary.SetDllImportResolver(typeof(Library).Assembly, (name, _, _) => handles.GetOrAdd(name, _ =>
                    {
                        if (!name.Contains("librdkafka", StringComparison.OrdinalIgnoreCase))
                        {
                            return IntPtr.Zero;
                        }

                        var files = directories.SelectMany(x => File.Exists(x) ? new[] { x }
                            : Directory.Exists(x) ? Directory.GetFiles(x, $"{name}.*") : Enumerable.Empty<string>());
                        foreach (var file in files)
                        {
                            try
                            {
                                var handle = NativeLibrary.Load(file);
                                Log.Trace($"CBOEDataQueueHandler.LoadNativeLibrary(): loaded {file}");
                                return handle;
                            }
                            catch (Exception exception)
                            {
                                // expected for builds which dependencies are not installed, the client will ask for the next one
                                Log.Trace($"CBOEDataQueueHandler.LoadNativeLibrary(): skipping {file}: {exception.Message.Split('\n')[0]}");
                            }
                        }
                        // default behavior
                        return IntPtr.Zero;
                    }));
                }

                // triggers the load
                Log.Trace($"CBOEDataQueueHandler.LoadNativeLibrary(): using librdkafka {Library.VersionString}");
            }
            catch (DllNotFoundException exception)
            {
                Log.Error($"CBOEDataQueueHandler.LoadNativeLibrary(): could not load the native kafka client 'librdkafka': {exception.Message}"
                    + " It can be set using 'cboe-librdkafka-location', by default it's searched for in the 'runtimes' folder or right next to the assemblies");
            }
            catch (Exception exception)
            {
                // we'll go with the default behavior
                Log.Error(exception);
            }
        }

        /// <summary>
        /// Determines whether the given topic is a Cboe Global Indices feed one, named '{channel}-idx-{region}', e.g. 'csmi-idx-use1'.
        /// The cluster serves other feeds too, which have their own format, and the index definitions topic we have no use for
        /// </summary>
        protected static bool IsIndexTopic(string topic)
        {
            return _indexTopicRegex.IsMatch(topic) && !topic.StartsWith("definition-", StringComparison.Ordinal);
        }

        /// <summary>
        /// Gets all the partitions of the given topics, or of all the available index topics if none, to be consumed starting from the latest value.
        /// We assign the partitions manually instead of joining the consumer group, so that each consumer gets every
        /// symbol, there are no group rebalances and we don't go through old values after being offline
        /// </summary>
        private static List<TopicPartitionOffset> GetPartitions(IConsumer<Ignore, string> consumer, string[] topics)
        {
            // uses the consumers connection, which remains its owner
            using var adminClient = new DependentAdminClientBuilder(consumer.Handle).Build();
            // holds the topics we are allowed to see
            var available = adminClient.GetMetadata(_metadataTimeout).Topics;
            if (topics.Length == 0)
            {
                topics = available.Select(x => x.Topic).Where(IsIndexTopic).ToArray();
            }

            var result = new List<TopicPartitionOffset>();
            foreach (var topic in topics)
            {
                var metadata = available.FirstOrDefault(x => x.Topic == topic);
                if (metadata == null || metadata.Error.IsError || metadata.Partitions.Count == 0)
                {
                    throw new InvalidOperationException($"Failed to get the partitions of topic '{topic}': {metadata?.Error.Code} - {metadata?.Error.Reason}."
                        + $" Available topics: {string.Join(",", available.Select(x => x.Topic))}");
                }
                result.AddRange(metadata.Partitions.Select(partition => new TopicPartitionOffset(topic, partition.PartitionId, Offset.End)));
            }

            if (result.Count == 0)
            {
                throw new InvalidOperationException($"Found no index topic to consume. Available topics: {string.Join(",", available.Select(x => x.Topic))}");
            }
            return result;
        }

        private void OnError(IConsumer<Ignore, string> consumer, Error error)
        {
            if (error.Code == ErrorCode.Local_AllBrokersDown)
            {
                // the client will keep on trying to reconnect, we are back once data flows again
                _connected = false;
            }
            Log.Error($"CBOEDataQueueHandler.OnError(): {error.Code} - {error.Reason}. IsFatal: {error.IsFatal}");
        }

        private static void OnLog(IConsumer<Ignore, string> consumer, LogMessage message)
        {
            if (message.Level <= SyslogLevel.Error)
            {
                Log.Error($"CBOEDataQueueHandler.OnLog(): {message.Facility} - {message.Message}");
            }
            else
            {
                Log.Trace($"CBOEDataQueueHandler.OnLog(): {message.Facility} - {message.Message}");
            }
        }

        private static void SetPemOrLocation(string value, Action<string> setPem, Action<string> setLocation)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return;
            }

            if (value.TrimStart().StartsWith(PemHeader, StringComparison.Ordinal))
            {
                // handle escaped new lines, PEM requires the real ones
                setPem(value.Replace("\\n", "\n"));
            }
            else if (File.Exists(value))
            {
                setLocation(value);
            }
            else
            {
                throw new FileNotFoundException($"CBOEDataQueueHandler: file not found '{value}'", value);
            }
        }

        private static bool CanSubscribe(SubscriptionDataConfig config)
        {
            return config.SecurityType == SecurityType.Index
                // international indices have their own market and are not served here, don't take them from other handlers
                && config.Symbol.ID.Market == Market.USA
                && config.TickType == TickType.Trade
                && !config.Symbol.IsCanonical()
                && config.Symbol.Value.IndexOfInvariant("universe", true) == -1;
        }

        private record IndexDetails(Symbol Symbol, DateTimeZone ExchangeTimeZone);

        private class ModulesReadLicenseRead : Api.RestResponse
        {
            [JsonProperty(PropertyName = "license")]
            public string License;

            [JsonProperty(PropertyName = "organizationId")]
            public string OrganizationId;
        }

        /// <summary>
        /// Validate the user of this project has permission to be using it via our web API.
        /// </summary>
        private static void ValidateSubscription()
        {
            try
            {
                const int productId = 462;
                var userId = Globals.UserId;
                var token = Globals.UserToken;
                var organizationId = Globals.OrganizationID;
                // Verify we can authenticate with this user and token
                var api = new ApiConnection(userId, token);
                if (!api.Connected)
                {
                    throw new ArgumentException("Invalid api user id or token, cannot authenticate subscription.");
                }
                // Compile the information we want to send when validating
                var information = new Dictionary<string, object>()
                {
                    {"productId", productId},
                    {"machineName", Environment.MachineName},
                    {"userName", Environment.UserName},
                    {"domainName", Environment.UserDomainName},
                    {"os", Environment.OSVersion}
                };
                // IP and Mac Address Information
                try
                {
                    var interfaceDictionary = new List<Dictionary<string, object>>();
                    foreach (var nic in NetworkInterface.GetAllNetworkInterfaces().Where(nic => nic.OperationalStatus == OperationalStatus.Up))
                    {
                        var interfaceInformation = new Dictionary<string, object>();
                        // Get UnicastAddresses
                        var addresses = nic.GetIPProperties().UnicastAddresses
                            .Select(uniAddress => uniAddress.Address)
                            .Where(address => !IPAddress.IsLoopback(address)).Select(x => x.ToString());
                        // If this interface has non-loopback addresses, we will include it
                        if (!addresses.IsNullOrEmpty())
                        {
                            interfaceInformation.Add("unicastAddresses", addresses);
                            // Get MAC address
                            interfaceInformation.Add("MAC", nic.GetPhysicalAddress().ToString());
                            // Add Interface name
                            interfaceInformation.Add("name", nic.Name);
                            // Add these to our dictionary
                            interfaceDictionary.Add(interfaceInformation);
                        }
                    }
                    information.Add("networkInterfaces", interfaceDictionary);
                }
                catch (Exception)
                {
                    // NOP, not necessary to crash if fails to extract and add this information
                }
                // Include our OrganizationId if specified
                if (!string.IsNullOrEmpty(organizationId))
                {
                    information.Add("organizationId", organizationId);
                }

                // Create HTTP request
                using var request = ApiUtils.CreateJsonPostRequest("modules/license/read", information);

                api.TryRequest(request, out ModulesReadLicenseRead result);
                if (!result.Success)
                {
                    throw new InvalidOperationException($"Request for subscriptions from web failed, Response Errors : {string.Join(',', result.Errors)}");
                }

                var encryptedData = result.License;
                // Decrypt the data we received
                DateTime? expirationDate = null;
                long? stamp = null;
                bool? isValid = null;
                if (encryptedData != null)
                {
                    // Fetch the org id from the response if it was not set, we need it to generate our validation key
                    if (string.IsNullOrEmpty(organizationId))
                    {
                        organizationId = result.OrganizationId;
                    }
                    // Create our combination key
                    var password = $"{token}-{organizationId}";
                    var key = SHA256.HashData(Encoding.UTF8.GetBytes(password));
                    // Split the data
                    var info = encryptedData.Split("::");
                    var buffer = Convert.FromBase64String(info[0]);
                    var iv = Convert.FromBase64String(info[1]);
                    // Decrypt our information
                    using var aes = Aes.Create();
                    var decryptor = aes.CreateDecryptor(key, iv);
                    using var memoryStream = new MemoryStream(buffer);
                    using var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
                    using var streamReader = new StreamReader(cryptoStream);
                    var decryptedData = streamReader.ReadToEnd();
                    if (!decryptedData.IsNullOrEmpty())
                    {
                        var jsonInfo = JsonConvert.DeserializeObject<JObject>(decryptedData);
                        expirationDate = jsonInfo["expiration"]?.Value<DateTime>();
                        isValid = jsonInfo["isValid"]?.Value<bool>();
                        stamp = jsonInfo["stamped"]?.Value<int>();
                    }
                }
                // Validate our conditions
                if (!expirationDate.HasValue || !isValid.HasValue || !stamp.HasValue)
                {
                    throw new InvalidOperationException("Failed to validate subscription.");
                }

                var nowUtc = DateTime.UtcNow;
                var timeSpan = nowUtc - Time.UnixTimeStampToDateTime(stamp.Value);
                if (timeSpan > TimeSpan.FromHours(12))
                {
                    throw new InvalidOperationException("Invalid API response.");
                }
                if (!isValid.Value)
                {
                    throw new ArgumentException($"Your subscription is not valid, please check your product subscriptions on our website.");
                }
                if (expirationDate < nowUtc)
                {
                    throw new ArgumentException($"Your subscription expired {expirationDate}, please renew in order to use this product.");
                }
            }
            catch (Exception e)
            {
                Log.Error($"CBOEDataQueueHandler.ValidateSubscription(): Failed during validation, shutting down. Error : {e.Message}");
                throw;
            }
        }
    }
}
