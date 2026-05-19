# ConnectionState.msg
string state            # "DISCONNECTED", "CONNECTING", "CONNECTED", "CONNECTION FAILED"
string active_ssid      # 현재 연결된 Wi-Fi 이름
string signal_status    # "강전계" (Strong), "약전계" (Weak)
int32  signal_strength  # 현재 신호 강도 (0 ~ 100)