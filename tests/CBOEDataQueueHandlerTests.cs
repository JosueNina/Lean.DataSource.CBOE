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
 *
*/

using System;
using System.Linq;
using System.Threading;
using System.Collections;
using System.Collections.Generic;
using QuantConnect.Configuration;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Logging;
using QuantConnect.Data.Market;
using QuantConnect.DataSource.DataQueueHandlers;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class CBOEDataQueueHandlerTests
    {
        private static readonly Symbol _spx = Symbol.Create("SPX", SecurityType.Index, Market.USA);

        private TestableCBOEDataQueueHandler _handler;

        [SetUp]
        public void SetUp()
        {
            // there's no connection attempt until the job is set
            _handler = new TestableCBOEDataQueueHandler();
        }

        [TearDown]
        public void TearDown()
        {
            _handler.Dispose();
        }

        [Test]
        public void IsNotConnectedUntilJobIsSet()
        {
            Assert.IsFalse(_handler.IsConnected);
        }

        [Test]
        public void EmitsIndexValue()
        {
            var enumerator = _handler.Subscribe(GetConfig(_spx), (_, _) => { });

            // spec example: holds bid and ask values too, which we ignore
            _handler.Process(@"{""sendTime"": 1635171838052, ""msgType"": ""X"", ""seq"": 95742, ""sym"": ""SPX"", ""values"": [
                { ""type"": ""3"", ""value"": 4551.11 }, { ""type"": ""0"", ""value"": 4549.77 }, { ""type"": ""1"", ""value"": 4552.14 }]}");

            var ticks = GetTicks(enumerator);

            Assert.AreEqual(1, ticks.Count);
            Assert.AreEqual(_spx, ticks[0].Symbol);
            Assert.AreEqual(TickType.Trade, ticks[0].TickType);
            Assert.AreEqual(4551.11m, ticks[0].Value);
            Assert.AreEqual(0, ticks[0].Quantity);
            // 2021-10-25 14:23:58.052 UTC
            Assert.AreEqual(new DateTime(2021, 10, 25, 10, 23, 58, 52), ticks[0].Time);
        }

        [Test]
        public void EmitsIndexValueWithStatus()
        {
            var enumerator = _handler.Subscribe(GetConfig(_spx), (_, _) => { });

            // numeric fields as strings and unknown properties are handled
            _handler.Process(@"{""msgType"": ""I"", ""sendTime"": ""1772637850632"", ""seq"": 10644423, ""sym"": ""SPX"", ""newField"": {""a"": 1},
                ""values"": [{ ""type"": 3, ""value"": ""35151.5027"" }, { ""type"": 4, ""value"": 35000 }], ""idxStatus"": ""N""}");

            var ticks = GetTicks(enumerator);

            Assert.AreEqual(1, ticks.Count);
            Assert.AreEqual(35151.5027m, ticks[0].Value);
        }

        [TestCase(@"{""sendTime"": 1635171646325, ""msgType"": ""X"", ""seq"": 91797, ""sym"": ""VIX"", ""values"": [{ ""type"": ""3"", ""value"": 15.73 }]}", Description = "Not subscribed")]
        [TestCase(@"{""msgType"": ""C"", ""sendTime"": 1646069383825, ""transTime"": 1646069382825, ""seq"": 100, ""sym"": ""SPX"", ""values"": [{ ""type"": ""3"", ""value"": 1.23 }]}", Description = "Contributor")]
        [TestCase(@"{""msgType"": ""S"", ""sendTime"": 1646069113466, ""transTime"": 1646069112466, ""effectiveDate"": ""20220228"", ""summaryType"": ""1"", ""seq"": 100, ""sym"": ""SPX"", ""values"": [{ ""type"": ""3"", ""value"": 1.23 }]}", Description = "Summary")]
        [TestCase(@"{""sendTime"": 1443707509082, ""msgType"": ""D"", ""seq"": 7565, ""sym"": ""SPX"", ""currRec"": 11, ""totRecs"": 500, ""desc"": ""S&P 500"", ""date"": ""2023-02-24"", ""status"": 1, ""agentCode"": ""CO"", ""channels"": [""CSMI""]}", Description = "Definition")]
        [TestCase(@"{""sendTime"": 1635171838052, ""msgType"": ""X"", ""seq"": 95742, ""sym"": ""SPX"", ""values"": [{ ""type"": ""0"", ""value"": 4549.77 }]}", Description = "Bid only")]
        [TestCase(@"{""msgType"": ""Z"", ""sym"": ""SPX""}", Description = "Unknown type")]
        [TestCase("not json", Description = "Invalid")]
        [TestCase("", Description = "Empty")]
        public void IgnoresMessage(string message)
        {
            var enumerator = _handler.Subscribe(GetConfig(_spx), (_, _) => { });

            Assert.DoesNotThrow(() => _handler.Process(message));

            Assert.AreEqual(0, GetTicks(enumerator).Count);
        }

        [Test]
        public void EmitsAllSymbolsToTheAggregator()
        {
            const string message = @"{""sendTime"": 1635171646325, ""msgType"": ""X"", ""seq"": 91797, ""sym"": ""VIX"", ""values"": [{ ""type"": ""3"", ""value"": 15.73 }]}";
            var spx = _handler.Subscribe(GetConfig(_spx), (_, _) => { });

            // no one wants it yet, the aggregator drops it
            _handler.Process(message);
            Assert.AreEqual(0, GetTicks(spx).Count);

            var vixSymbol = Symbol.Create("VIX", SecurityType.Index, Market.USA);
            var vix = _handler.Subscribe(GetConfig(vixSymbol), (_, _) => { });
            _handler.Process(message);

            var ticks = GetTicks(vix);
            Assert.AreEqual(1, ticks.Count);
            Assert.AreEqual(vixSymbol, ticks[0].Symbol);
            Assert.AreEqual(15.73m, ticks[0].Value);
            Assert.AreEqual(0, GetTicks(spx).Count);
        }

        [Test]
        public void StopsEmittingAfterLastUnsubscribe()
        {
            const string message = @"{""sendTime"": 1635171838052, ""msgType"": ""X"", ""sym"": ""SPX"", ""values"": [{ ""type"": ""3"", ""value"": 4551.11 }]}";
            var tickConfig = GetConfig(_spx);
            var minuteConfig = GetConfig(_spx, Resolution.Minute);
            var enumerator = _handler.Subscribe(tickConfig, (_, _) => { });
            _handler.Subscribe(minuteConfig, (_, _) => { });

            _handler.Unsubscribe(minuteConfig);
            _handler.Process(message);
            Assert.AreEqual(1, GetTicks(enumerator).Count);

            _handler.Unsubscribe(tickConfig);
            _handler.Process(message);
            Assert.AreEqual(0, GetTicks(enumerator).Count);
        }

        [Test]
        public void OnlySubscribesIndexTrades()
        {
            Assert.IsNull(_handler.Subscribe(GetConfig(Symbol.Create("EURUSD", SecurityType.Forex, Market.Oanda)), (_, _) => { }));
            Assert.IsNull(_handler.Subscribe(GetConfig(_spx, tickType: TickType.Quote), (_, _) => { }));
            // international indices are not served
            Assert.IsNull(_handler.Subscribe(GetConfig(Symbol.Create("HSI", SecurityType.Index, Market.HKFE)), (_, _) => { }));
        }

        [TestCase("csmi-idx-use1", true)]
        [TestCase("cccy-idx-ape1", true)]
        [TestCase("cgi-idx-euw2", true)]
        [TestCase("ftse-idx-use1", true)]
        [TestCase("mstarc-idx-use1", true)]
        // index definitions
        [TestCase("definition-idx-use1", false)]
        // other feeds served by the same cluster
        [TestCase("coa-opra-idx-cgifmain-filt-ape1", false)]
        [TestCase("cos-b1s-use1", false)]
        [TestCase("bzx-tbc-use1", false)]
        [TestCase("cfe-mtop-use1", false)]
        [TestCase("cboe-options-lite-listed-01-ape1", false)]
        [TestCase("__consumer_offsets", false)]
        public void DetectsIndexTopics(string topic, bool expected)
        {
            Assert.AreEqual(expected, TestableCBOEDataQueueHandler.IsIndex(topic));
        }

        [Test]
        public void SetJobThrowsIfConfigurationIsMissing()
        {
            Assert.Throws<ArgumentException>(() => _handler.SetJob(new Packets.LiveNodePacket { BrokerageData = new Dictionary<string, string>() }));
        }

        [Test]
        public void LoadsNativeLibrary()
        {
            TestableCBOEDataQueueHandler.LoadNative();

            Assert.IsTrue(Confluent.Kafka.Library.IsLoaded);
            Log.Trace($"librdkafka version: {Confluent.Kafka.Library.VersionString}");
        }

        [Test]
        public void SetJobHandlesMissingBrokerageData()
        {
            // the job might have no brokerage data, we fallback to the configuration, which is not set here
            Assert.Throws<ArgumentException>(() => _handler.SetJob(new Packets.LiveNodePacket { BrokerageData = null }));
            Assert.Throws<ArgumentException>(() => _handler.SetJob(null));
        }

        /// <summary>
        /// Requires the 'cboe-*', 'job-user-id', 'api-access-token' and 'data-folder' configuration values to be set,
        /// in the config.json file or as 'QC_*' environment variables, e.g. 'QC_CBOE_GROUP_ID'
        /// </summary>
        [Test, Explicit("Requires Cboe Global Cloud credentials and connectivity")]
        public void StreamsData()
        {
            foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables())
            {
                var key = entry.Key.ToString();
                if (key.StartsWith("QC_"))
                {
                    Config.Set(key.Substring(3).Replace("_", "-").ToLowerInvariant(), entry.Value.ToString());
                }
            }
            // reloads the user id and token, used to validate the subscription
            Globals.Reset();

            using var handler = new CBOEDataQueueHandler();
            handler.SetJob(new Packets.LiveNodePacket());
            var enumerators = new[] { "SPX", "VIX" }
                .Select(ticker => handler.Subscribe(GetConfig(Symbol.Create(ticker, SecurityType.Index, Market.USA)), (_, _) => { }))
                .ToList();

            var count = 0;
            var end = DateTime.UtcNow.AddSeconds(60);
            while (DateTime.UtcNow < end)
            {
                foreach (var tick in enumerators.SelectMany(GetTicks))
                {
                    count++;
                    Log.Trace($"{tick.Time:HH:mm:ss.fff} {tick.Symbol} {tick.Value}");
                }
                Thread.Sleep(100);
            }

            Assert.IsTrue(handler.IsConnected);
            Assert.Greater(count, 0);
        }

        private static SubscriptionDataConfig GetConfig(Symbol symbol, Resolution resolution = Resolution.Tick, TickType tickType = TickType.Trade)
        {
            return new SubscriptionDataConfig(typeof(Tick), symbol, resolution, TimeZones.NewYork, TimeZones.NewYork, false, false, false, tickType: tickType);
        }

        private static List<Tick> GetTicks(IEnumerator<BaseData> enumerator)
        {
            var result = new List<Tick>();
            while (enumerator.MoveNext() && enumerator.Current != null)
            {
                result.Add((Tick)enumerator.Current);
            }
            return result;
        }

        private class TestableCBOEDataQueueHandler : CBOEDataQueueHandler
        {
            public void Process(string message) => ProcessMessage(message);

            public static bool IsIndex(string topic) => IsIndexTopic(topic);

            public static void LoadNative() => LoadNativeLibrary();

            // there is no market hours database available
            protected override NodaTime.DateTimeZone GetExchangeTimeZone(Symbol symbol) => TimeZones.NewYork;
        }
    }
}
