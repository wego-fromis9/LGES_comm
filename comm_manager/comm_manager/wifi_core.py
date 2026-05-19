import subprocess

class WifiCore:
    def __init__(self, config, logger):
        """
        초기화 메서드
        ROS 2 노드로부터 config 데이터와 logger 객체를 주입(Dependency Injection) 받습니다.
        """
        self.logger = logger
        self.network_config = config.get('network', {})
        self.base_ssid = self.network_config.get('ssid', '')
        self.password = self.network_config.get('password', '')
        self.min_signal = self.network_config.get('min_signal_threshold', 40)
        
        # 5G 대체망 이름 자동 설정 로직
        if self.base_ssid:
            self.alt_ssid = self.base_ssid[:-3] if self.base_ssid.endswith('_5G') else self.base_ssid + '_5G'
        else:
            self.alt_ssid = ''

    def get_signal_status(self, signal_level):
        """신호 강도 숫자를 받아 상태 문자열을 반환합니다."""
        if signal_level <= 0:
            return "Error"
        return "Strong" if signal_level >= self.min_signal else "Weak"

    def get_active_info(self):
        """
        현재 리눅스 커널에서 활성화된 SSID와 신호 강도(%)를 추출하여 반환합니다.
        반환값: (ssid_string, signal_integer)
        """
        try:
            res = subprocess.check_output("nmcli -t -f active,ssid,signal dev wifi | grep '^yes'", shell=True)
            parts = res.decode('utf-8').strip().split(':')
            if len(parts) >= 3:
                return parts[1], int(parts[2])
        except Exception:
            pass
        return "", 0

    def connect_initial(self):
        """
        초기 Wi-Fi 연결을 수행합니다. (기본망 -> 대체망 순차 시도)
        반환값: 연결 성공 여부 (True/False)
        """
        if not self.base_ssid:
            self.logger.warn("설정 파일(config.yaml)에 SSID가 지정되지 않았습니다.")
            return False

        self.logger.info(f"Wi-Fi 연결 시도 (기본망): {self.base_ssid}")
        res = subprocess.run(f"nmcli dev wifi connect '{self.base_ssid}' password '{self.password}'", shell=True, stdout=subprocess.DEVNULL)
        
        if res.returncode == 0:
            self.logger.info(f"기본망 Wi-Fi 연결 성공: {self.base_ssid}")
            return True
            
        self.logger.warn(f"기본망 연결 실패. 동일 비번 대체망({self.alt_ssid}) 시도 중...")
        res_alt = subprocess.run(f"nmcli dev wifi connect '{self.alt_ssid}' password '{self.password}'", shell=True, stdout=subprocess.DEVNULL)
        
        if res_alt.returncode == 0:
            self.logger.info(f"대체망 Wi-Fi 연결 성공: {self.alt_ssid}")
            return True
        
        self.logger.error("Wi-Fi 연결 완전 실패 (기본망/대체망 모두 접속 불가)")
        return False

    def roam_to_better_network(self, current_ssid, current_signal):
        """
        주변 망을 스캔하여 대체망의 신호가 현재보다 15% 이상 강하면 강제 로밍(스위칭)합니다.
        반환값: 로밍 실행 여부 (True/False)
        """
        try:
            scan = subprocess.check_output("nmcli -t -f SSID,SIGNAL dev wifi", shell=True).decode('utf-8')
            signals = {}
            for line in scan.split('\n'):
                if ':' in line:
                    s, sig = line.split(':')
                    if s in [self.base_ssid, self.alt_ssid]: 
                        signals[s] = int(sig)

            target_ssid = self.base_ssid if current_ssid == self.alt_ssid else self.alt_ssid

            # 15% 이상 신호가 강할 때만 핑퐁 현상 없이 스위칭
            if target_ssid in signals and signals[target_ssid] > current_signal + 15:
                self.logger.info(f"스위칭 시도 ({current_ssid}:{current_signal}% -> {target_ssid}:{signals[target_ssid]}%)")
                subprocess.run(f"nmcli dev wifi connect '{target_ssid}' password '{self.password}'", shell=True, stdout=subprocess.DEVNULL)
                return True
        except Exception as e:
            self.logger.debug(f"로밍 스캔 중 오류 발생: {e}")
            
        return False

    def monitor_and_roam(self):
        """
        타이머나 메인 루프에서 주기적으로 호출되어 단절을 감지하고 로밍을 판단하는 래퍼(Wrapper) 함수.
        반환값: (연결_유지_여부_bool, 현재_SSID, 현재_신호강도, 상태문자열)
        """
        if not self.base_ssid:
            return False, "", 0, "Disconnected"

        current_ssid, current_signal = self.get_active_info()

        # 1. 물리적 단절 감지 (현재 붙어있는 망이 우리 망이 아닐 때)
        if current_ssid not in [self.base_ssid, self.alt_ssid]:
            self.logger.error(f"와이파이 단절 감지 (현재 이탈 망: {current_ssid if current_ssid else '없음'})")
            success = self.connect_initial()
            
            if success:
                current_ssid, current_signal = self.get_active_info()
                return True, current_ssid, current_signal, self.get_signal_status(current_signal)
            else:
                return False, "", 0, "Disconnected"

        # 2. 신호 저하 감지 및 지능형 로밍(스위칭)
        if current_signal < self.min_signal:
            self.logger.warn(f"현재 신호 약함 ({current_signal}% < 임계값 {self.min_signal}%). 강전계 탐색 중...")
            is_roamed = self.roam_to_better_network(current_ssid, current_signal)
            
            if is_roamed:
                # 로밍에 성공했다면 바뀐 정보를 다시 취득
                current_ssid, current_signal = self.get_active_info()
        
        # 상태 판정 추가
        status_str = self.get_signal_status(current_signal)
        
        # (유지_여부, SSID, 신호강도, 상태문자열) 4개를 반환하도록 확장
        return True, current_ssid, current_signal, status_str
