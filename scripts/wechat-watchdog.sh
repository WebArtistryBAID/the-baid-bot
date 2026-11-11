#!/system/bin/sh
# Relaunches WeChat's main process whenever it isn't running, and keeps the phone's
# processor awake so the checks aren't paused by deep sleep. The screen can still
# turn off. Meant for a phone that stays on its charger.
#
# Install on the phone (KernelSU runs scripts in this folder after every start-up):
#   /data/adb/boot-completed.d/wechat-watchdog.sh  (mode 755)
# Relaunches are logged to /data/adb/wechat-watchdog.log.
#
# To release the wake lock by hand (for example after killing this script with -9):
#   echo wechat_watchdog > /sys/power/wake_unlock

LOG=/data/adb/wechat-watchdog.log
WAKE_LOCK=wechat_watchdog
# /dev is cleared at every start-up, so this lock only stops a second copy running
# at the same time.
LOCK=/dev/wechat-watchdog.lock

if ! mkdir "$LOCK" 2>/dev/null; then
  exit 0
fi

stop() {
  echo "$WAKE_LOCK" > /sys/power/wake_unlock 2>/dev/null
  rmdir "$LOCK" 2>/dev/null
  echo "$(date '+%F %T') watchdog stopped, wake lock released" >> "$LOG"
  exit 0
}
trap stop INT TERM HUP

# Wait until the phone has been unlocked once since starting up. Before that, shared
# storage is unavailable and WeChat can't load the account.
until [ -d /storage/emulated/0/Android ]; do sleep 10; done

echo "$WAKE_LOCK" > /sys/power/wake_lock
echo "$(date '+%F %T') watchdog started, wake lock held" >> "$LOG"

while true; do
  # Renewed every minute in case anything else cleared it.
  echo "$WAKE_LOCK" > /sys/power/wake_lock
  if ! pidof com.tencent.mm >/dev/null; then
    echo "$(date '+%F %T') WeChat not running, starting it" >> "$LOG"
    am start -n com.tencent.mm/.ui.LauncherUI >/dev/null 2>&1
  fi
  # Sleep in the background and wait for it, so a stop signal is handled at once.
  sleep 60 &
  wait $!
done
