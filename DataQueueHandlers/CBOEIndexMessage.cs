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

using System.Collections.Generic;
using Newtonsoft.Json;

namespace QuantConnect.DataSource.DataQueueHandlers
{
    /// <summary>
    /// Cboe Global Indices feed message, see the 'Cboe Titanium Cboe Global Cloud Feed Specification'
    /// </summary>
    public class CBOEIndexMessage
    {
        /// <summary>
        /// The message type: X = index value, I = index value with status, C = contributor value, S = summary, D = definition
        /// </summary>
        [JsonProperty("msgType")]
        public string MessageType { get; set; }

        /// <summary>
        /// Epoch time in milliseconds the value was published at
        /// </summary>
        [JsonProperty("sendTime")]
        public long SendTime { get; set; }

        /// <summary>
        /// The index symbol
        /// </summary>
        [JsonProperty("sym")]
        public string Symbol { get; set; }

        /// <summary>
        /// The index value blocks
        /// </summary>
        [JsonProperty("values")]
        public List<CBOEIndexValue> Values { get; set; }
    }

    /// <summary>
    /// Cboe Global Indices feed index value block
    /// </summary>
    public class CBOEIndexValue
    {
        /// <summary>
        /// The value type: 0 = bid, 1 = ask, 3 = index value, 4 = close value, 5 = settlement price
        /// </summary>
        [JsonProperty("type")]
        public int Type { get; set; }

        /// <summary>
        /// The value associated with the data point
        /// </summary>
        [JsonProperty("value")]
        public decimal Value { get; set; }
    }
}
