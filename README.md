# Lean.DataSource.CBOE
Example production implementation for our data marketplace CBOE indexes dataset.

# Live data: Cboe Global Cloud
`DataQueueHandlers` holds `CBOEDataQueueHandler`, an `IDataQueueHandler` implementation streaming live index values from the
[Cboe Global Cloud](https://www.cboe.com/document/tech-spec/content/technical-specifications/cboe-titanium-cboe-global-cloud-feed-specification)
*Cboe Global Indices* feed: a Kafka cluster hosted in AWS, authenticated using mutual TLS. It serves `SecurityType.Index` trade subscriptions, e.g. `AddIndex("SPX")`.

To use it set `"data-queue-handler": [ "QuantConnect.DataSource.DataQueueHandlers.CBOEDataQueueHandler" ]` and the following configuration values,
which can also be provided through the live job `BrokerageData`, which takes precedence. All are provided by Cboe during the onboarding process.
The connection is started once the job is set, see `SetJob`.

| Key | Description |
| --- | --- |
| `cboe-bootstrap-servers` | Comma separated list of `<broker FQDN>:9094` |
| `cboe-group-id` | The assigned consumer group id, case sensitive |
| `cboe-ssl-certificate` | The client certificate: path to the PEM file or the PEM content itself |
| `cboe-ssl-key` | The client private key: path to the PEM file or the PEM content itself |
| `cboe-ssl-key-password` | The private key passphrase, if any |
| `cboe-topics` | Optional, comma separated list of topics to consume, e.g. `csmi-idx-use1`, the main indices channel (SPX, VIX...) in us-east-1. Defaults to all the index topics (`<channel>-idx-<region>`) the certificate has access to |
| `cboe-ssl-ca` | Optional, CA certificate to verify the brokers with. Defaults to the system trusted root certificates |
| `cboe-librdkafka-location` | Optional, file or directory to load the native kafka client (`librdkafka`) from. By default it's searched for in the `runtimes` folder, or right next to, the assemblies |

Notes:
- When using the *Internet Gateway* connectivity method, the broker FQDNs have to resolve to the public IP addresses provided by Cboe,
  for example adding them to the hosts file. The source IP address has to be allowed by Cboe.
- When deploying, `Confluent.Kafka.dll` and the `runtimes` folder holding the native kafka client (`librdkafka`) have to be copied along with the assembly, the build output holds them all.
- There are no per symbol subscriptions in the feed: the configured topics are consumed entirely and every index value is sent to the data aggregator,
  which drops those without a subscription. The exchange time zones are taken from the market hours database.
- The handler always starts streaming from the latest value and does not join the consumer group, so multiple instances can run at the same time, each getting every symbol.

# Implementing your own data source
To learn more about implementing your own data source for our marketplace, visit the [LeanDataSdk](https://github.com/QuantConnect/LeanDataSdk) repository for more information.
