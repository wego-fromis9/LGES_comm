# comm_manager Config Guide

`config.yaml`은 include 목록만 가진 root 파일입니다. 실제 운영 값은 목적별 YAML 파일을 수정합니다.

## Files

| File | Purpose |
|---|---|
| `network.yaml` | Wi-Fi/network 관리 설정 |
| `mqtt.yaml` | broker, base topic, 공통 header, keep alive, LWT, TLS/auth |
| `time_sync.yaml` | broker `sync` topic 기반 OS 시간 동기화 |
| `inbound.yaml` | order/instantActions 수신 후 ROS service routing |
| `outbound.yaml` | state/factsheet/visualization 발행과 ROS topic/service source |
| `runtime.yaml` | robot identity와 lock file |
| `messages/*.yaml` | MQTT message별 topic suffix, QoS, retain, payload 기본값 |

## MQTT Base Topic

`mqtt.yaml`:

```yaml
mqtt:
  topic:
    base: uagv/v2/{manufacturer}/{serialNumber}
```

사용 가능한 placeholder:

- `{manufacturer}`
- `{serialNumber}`
- `{serial_number}`
- `{version}`

## Common Header

모든 MQTT payload에 들어가는 공통 header는 `mqtt.yaml`에서 관리합니다.

```yaml
mqtt:
  header:
    fields:
      headerId:
        source: counter
      timestamp:
        source: timestamp
      version:
        value: 2.0.0
      manufacturer:
        value: WEGO
      serialNumber:
        value: AMR-001
```

## Message Payload

각 MQTT message의 topic/QoS/retain/payload 기본값은 `messages/*.yaml`에서 수정합니다.

예: `messages/connection.yaml`

```yaml
messages:
  connection:
    meta:
      topic_suffix: connection
      qos: 1
      retain: true
    payload:
      connectionState: CONNECTED
```

Python 코드는 기존 JSON template을 먼저 읽고, 이후 `messages/*.yaml`의 값을 덮어씁니다. 즉, 운영 중 바뀔 가능성이 큰 값은 YAML에서 수정하면 됩니다.

## Message Field Mapping

`messages/*.yaml`의 `fields`는 해당 MQTT payload 필드가 어디에서 오는지 적는 운영 매핑표이며, comm_manager가 실제 발행 payload를 만들 때 이 값을 사용합니다.

- `connection`, `state`, `factsheet`, `visualization`, `response`는 comm_manager가 MQTT로 발행합니다.
- `state`, `factsheet`, `visualization`의 실제 ROS source topic/service 이름은 `outbound.yaml`에서 바꿉니다.
- `order`, `instantActions`, `sync`는 broker/host에서 들어오는 inbound 메시지입니다.
- `order`와 `instantActions`는 ROS topic 데이터를 채워 발행하는 대상이 아닙니다. 대신 `inbound.yaml`의 service routing에 따라 `/recipe/run`, `/recipe/instant_action` 등으로 전달됩니다.
- inbound service request도 `inbound.yaml`의 `request_mapping`을 사용합니다. 예를 들어 `orderId -> /recipe/run.order_id`, `recipeId -> /recipe/run.recipe_id` 같은 연결은 코드가 아니라 YAML에서 바꿉니다.

현재 주요 source는 다음과 같습니다.

| MQTT message | Direction | Source |
|---|---|---|
| `connection` | outbound | comm_manager runtime connection state |
| `state` | outbound | `/mir/state`, `/mir/errors_json`, `/mir/current_waypoint_json`, `/recipe/state`, `/system/safety_state`, `/recipe/query_list` |
| `factsheet` | outbound | `/mir/waypoints_json`, `/mir/state`, `/recipe/query_list` |
| `visualization` | optional outbound | latest `state` header id, robot pose, velocity |
| `response` | outbound response | inbound validation result and ROS service result |
| `sync` | inbound | broker timestamp payload on MQTT `sync` topic |
| `order` | inbound | MQTT order payload routed to `/recipe/run` |
| `instantActions` | inbound | MQTT instantActions payload routed to `/recipe/instant_action` or internal factsheet publish |

## Outbound Mapping Example

`messages/state.yaml`:

```yaml
messages:
  state:
    payload:
      robotPosition: null
    fields:
      robotPosition:
        source: ros_topic
        ros_topic: /mir/state
        ros_type: std_msgs/msg/String
        payload_format: json
        object_fields:
          x:
            field_path: position.x
          y:
            field_path: position.y
          theta:
            field_path: position.orientation
            transform: degrees_to_radians_round_3
```

위처럼 template payload에 항목이 있고 `fields`에 source가 있으면 comm_manager가 topic 값을 읽어 MQTT JSON에 채웁니다.

## Inbound Mapping Example

`inbound.yaml`:

```yaml
inbound:
  order:
    service: /recipe/run
    request_mapping:
      order_id:
        source: payload
        fields: [orderId]
      recipe_id:
        source: payload
        fields: [recipeId]
      input_json:
        source: payload_json
```

위처럼 MQTT `order`에서 받은 JSON 필드를 ROS service request 필드에 연결합니다.

## Build Exclusion

`comm_debug_tools`는 현재 runtime build 대상에서 제외했습니다.

```text
/home/wego/LGES_ws/src/comm_debug_tools/COLCON_IGNORE
```

다시 빌드에 포함하려면 위 파일을 삭제하면 됩니다.
