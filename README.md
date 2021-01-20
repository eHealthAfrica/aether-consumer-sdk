# Aether Kafka SDK Features
Aether Output Connectors are a powerful way of feeding foreign systems with data from an Aether instance. Part of our approach with Aether is to provide simple access to powerful functionality without being overly opinionated. With the Aether Connect SDK, we provide a Jobs based, core application and a low level Python API to Kafka Topics running on the instance. This ensures all Aether Consumers are driven by the same basic interactions, using the same interface and allow for things like security and multi-tenancy of consumers to be considered once instead of being implemented in each Connector. On top of the base functionality you would expect from a Kafka Consumer, we provide a number of additional features.

  - A consistent API and Data Model for all Aether consumers, including:
    - Schema defined Jobs and Resources for your particular consumer.
    - Resource API and persistence in Redis
    - Job API and persistence in Redis
    - A Base Job class that dynamically watches a Resource for changes.
    - Multi-tenancy support for Job and Resource types.
  - Extension of the basic Kafka Consumer to include.
    - Fast deserialization of messages with Spavro
    - Value based filtering of whole messages.
    - Exclusion of fields from a read message based on data classification


_To get started, grab the package from pip:_
```pip install aet.consumer```

# Base SDK Features

## Data Types

The SDK includes a basic data model consisting of two types.

  - *Resources*
    - A resource is a dependency that multi `Jobs` might rely on. For example, if we're creating a RDBMS connector, a resource could contain the connection information and credentials for a database instance, like:
    ```json
    {
        "id": "payroll-postgres",
        "url": "http://my.org:5432",
        "user": "user",
        "password": "password"
    }
    ```

  - *Jobs*
    - A job is the core unit of work that a consumer will perform. What a job does it entirely dependant on your implementation. In an RDBMS connector, a job could monitor a `topic` called `employees` and send that data to a `table` called `employees` in an external RDBMS called `payroll-postgres` that we have defined as a `Resource`.
    ```json
    {
        "id": "forward-employees-to-pg",
        "table": "employees",
        "topic": "employees",
        "resources": ["payroll-postgres"]
    }
    ```

### Data Type Definition

Each implementation will need something different from `Resource` a `Job`. As such, we need to define a schema for what a valid `Job` and `Resource` should consist of. The schema itself is a JSONSchema that describes the valid structure for the type.

The schemas for each type should be placed in `aet.consumer.BaseConsumer[schemas] (:Dict[str, Any])`. Generally, it's best to subclass BaseConsumer, and override any methods you'd like, including the `schemas` property. Every type registered in `aet.api.APIServer[_allowed_types] (:ClassVar[Dict[str, List]])` needs to have a schema registered in the subclassed Consumer. In this way, you can add more than one resource type, or other things you want persisted, and provide an API to CRUD and validate those resources. Unless you really need it, it's recommended that you stick to `Resource` and `Job`.

### API

The API operates on the types defined in the previous section. All registered types get the following behaviors by default:
`'READ', 'CREATE', 'DELETE', 'LIST', 'VALIDATE', 'SCHEMA'`.
In addition, `Jobs` and only Jobs get the methods:
`'PAUSE', 'RESUME', 'STATUS'`

The exposed endpoints are as follow:

_All Types_:

| Endpoint            |  Operation    | Allowed Methods                 |
| ------------------- | :-----------: | ------------------------        |
| `/{type}/add`       | `CREATE`      | `[POST as json]`                |
| `/{type}/delete`    | `DELETE`      | `[POST or GET/DELETE ?id={id}]` |
| `/{type}/update`    | `CREATE`      | `[POST as json]`                |
| `/{type}/validate`  | `VALIDATE`    | `[POST as json]`                |
| `/{type}/schema`    | `SCHEMA`      | `[POST or GET ?id={id}]`        |
| `/{type}/get`       | `READ`        | `[POST or GET ?id={id}]`        |
| `/{type}/list`      | `GET (all)`   | `[GET]`                         |

_Jobs only_:

| Endpoint            |  Operation    | Allowed Methods                 |
| ------------------- | :-----------: | ------------------------        |
| `/{type}/pause`     | `PAUSE`       | `[POST or GET ?id={id}]`        |
| `/{type}/resume`    | `RESUME`      | `[POST or GET ?id={id}]`        |
| `/{type}/status`    | `STATUS`      | `[POST or GET ?id={id}]`        |

For example, to get the `job` with `id = 0001` you send a GET request to:
`http://consumer-url/job/get?id=0001`


### Implementing Consumer Behavior

Once you have schemas defined, you'll need to implement the behavior that makes the consumer special. The BaseJob at `aet.job.BaseJob` and BaseConsumer at `aet.consumer.BaseConsumer` should be sub-classed.


#### `aet.api.APIServer`

This is the main application entry point. It needs, among other things, an instance of `BaseConsumer` or its subclass for initialization. Starting the APIServer runs everything until `stop()` is called.

#### `aet.consumer.BaseConsumer`:

  - Property:`schemas` -> Make sure to add your schemas here.
  - In the `__init__` function, change the `JobManager` initialization to take your subclass implementation of `BaseJob` as the `job_class`.


#### `aet.job.BaseJob`

Here is where you'll need to implement your own handling logic. To understand what the Job does once it's been spawned, look at the `_run` method to see the order of operations. The following methods should be overwritten in all cases:

  - `_get_messages(self, config, resources) -> List[message]`: Implement getting messages from Kafka based on the configuration and watched resources and returning them as a list depending on the requirements of your consumer.
  - `_handle_messages(self, config, resources, messages) -> None`: Implement what should happen to the messages that were fetched from Kafka in `_get_messages`
  - `_handle_new_settings() -> None` Implement logic for what happens when one of the `Resources` that this `Job` depends on changes. In most cases, this is nothing since the resource is passed to the `_handle_messages` method, but you may need to, for example, initialize a new connection.


# KafkaConsumer Extension Features

The SDK features extending the KafkaConsumer are built on top of the standard [kafka-python] library. The entire API remains intact with the following modifications.

## Deserialization with Spavro

There are many Avro libraries available for python. [Spavro] uses the syntax of the official python2 Avro library, while adding compatibility for Python3, and providing a 15x (de)serialization speed increase via C extensions. To support this functionality, we provide a `poll_and_deserialize(timeout_ms, max_records)` method, which mirrors the basic functionality of the `poll()` method from the kafka-python library, while providing deserialized messages in the following format:
```python
{
    "topic:{topic}-partition:{partition}" : [
        {
            "schema" : message_schema,
            "messages": [messages],
        },
    ],
}
```
For example, we can poll for the latest 100 messages like this:
```python
from aet.kafka import KafkaConsumer
consumer = KafkaConsumer(**config)
consumer.subscribe('my-topic')

new_records = consumer.poll_and_deserialize(
    timeout_ms=10,
    max_records=100
)

for partition_key, packages in new_records.items():
    for package in packages:
        schema = package.get('schema')
        messages = package.get('messages')
        if schema != last_schema:
            pass  # Or do something since the schema has changed
        for msg in messages:
            pass  # do something with each message
        last_schema = schema
```

Since any filtering based on the contents of a message require comprehension of the message, to perform any reads that requires filtering, _you must use this method_. Poll will return messages that are not filtered, regardless of consumer setting.

## Filtering Functionality

It is a common requirement to take a subset of the data in a particular topic and make it available to downstream systems via an Output Connector. There are two general classes of filtering that we support.

- Message Emit Filtering: Filtering of whole messages based on one of the values in the message's fields.
- Field Masking Filtering: Removing some fields from a message based on a classification and privledge system.

## Message Emit Filtering

Filtering of whole messages is based on a field value contained in a message matching one of a set of predefined values. Emit filtering is not controlled strictly by a messages schema. It is controlled by the following configuration values, set through the KafkaConsumer constructor. The only requisite is that a message contain the proper JSONPath.
```python
{
    "aether_emit_flag_required": True,
    "aether_emit_flag_field_path": "$.approved",
    "aether_emit_flag_values": [True],
}
```

Emit filtering is enabled by default through `"aether_emit_flag_required" : True`. If you messages will not require filtering in this manner, set it to `False` Once a message is deserialized, the consumer finds the value housed at the JSONPath `aether_emit_flag_field_path`. If that value is a member of the set configured in `aether_emit_flag_values`. A small example, given the default configuration shown above.

```json
{
    "id": "passing_message",
    "values": [1,2,3],
    "approved": true
}
```

This message would be emitted for downstream systems.

```json
{
	"id": "failing_message",
	"values": [1, 2, 3],
	"approved": false
}
```

This message would not be made available, since the value at path `$.approved` is `False` which is not a member of `[True]`.

It is not required that `aether_emit_flag_values` be a boolean. For example this is a valid configuration:

```python
{
    "aether_emit_flag_field_path": "$.record_type",
    "aether_emit_flag_values": ["routine_vaccination", "antenatal_vaccination"],
}
```
This message will be emitted.
```json
{
    "id": "passing_message",
    "date": "2018-07-01",
    "record_type": "routine_vaccination"
}
```
This message will _not_ be emitted.
```json
{
    "id": "failing_message",
    "date": "2018-07-01",
    "record_type": "postpartum_followup"
}
```

## Field Masking Filter

It is often a requirement that only a subset of a message be made available to a particular downstream system. In this case, we use field filtering. Field filtering requires an annotation in the Avro Schema of a message type on each field which might need to be stripped. This also implies that we have a information classification system which is appropriate for our data. For example, we could use this scale for the classification of governmental information `["public", "confidential", "secret", "top secret", "ufos"]` Where public information is the least sensitive, all the way up to highly classified information about the existence of UFOs.

_Having these classifiers, we can use them in the Avro schema for a message type._
```json
{
    "fields": [
        {
            "jsonldPredicate": "@id",
            "type": "string",
            "name": "id"
        },
        {
            "type": [
                "null",
                "boolean"
            ],
            "name": "publish",
        },
        {
            "type": [
                "null",
                "string"
            ],
            "name": "field1",
            "aetherMaskingLevel": "public"
        },
        {
            "type": [
                "null",
                "string"
            ],
            "name": "field2",
            "aetherMaskingLevel": "confidential"
        },
        {
            "type": [
                "null",
                "string"
            ],
            "name": "field3",
            "aetherMaskingLevel": "secret"
        },
        {
            "type": [
                "null",
                "string"
            ],
            "name": "field4",
            "aetherMaskingLevel": "top secret"
        },
        {
            "type": [
                "null",
                "string"
            ],
            "name": "field5",
            "aetherMaskingLevel": "ufos"
        },
    ],
    "type": "record",
    "name": "TestTopSecret"
}
```
Now we have the following message in a topic:
```json
{
    "id" : "a_guid",
    "publish" : true,
    "field1" : "a",
    "field2" : "b",
    "field3" : "c",
    "field4" : "d",
    "field5" : "e"
}
```
If we use an emit level of `"aether_masking_schema_emit_level": "public"` in the following configuration, only fields with that classification or less (including no classification) will be emitted.
```python
{
    "aether_emit_flag_required": True,
    "aether_emit_flag_field_path": "$.publish",
    "aether_emit_flag_values": [True],
    "aether_masking_schema_levels" : ["public", "confidential", "secret", "top secret", "ufos"],
    "aether_masking_schema_emit_level": "public",
}
```
_The following message will be emitted:_
```json
{
    "id" : "a_guid",
    "publish" : true,
    "field1" : "a"
}
```
If the rest of the configuration remains, but we use `"aether_masking_schema_emit_level": "secret"` the message becomes.
```json
{
    "id" : "a_guid",
    "publish" : true,
    "field1" : "a",
    "field2" : "b",
    "field3" : "c"
}
```
In this way, we can have different consumers emitting different versions of the same message to their respective downstream systems.

[kafka-python]: <https://github.com/dpkp/kafka-python>
[spavro]: <https://github.com/pluralsight/spavro>
