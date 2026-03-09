import subprocess
from datetime import datetime
import pytz


def run_sar(*args):
    """Run sar command safely without shell. Pass args as: run_sar('sar', '-u', '-f', filepath)"""
    result = subprocess.run(
        list(args),
        capture_output=True,
        text=True,
        shell=False,
    )
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            args,
            result.stdout,
            result.stderr,
        )
    lines = result.stdout.splitlines()

    data = []
    for line in lines:
        parts = line.split()

        if not parts:
            continue

        if parts[0] == "Linux":
            continue

        if "%user" in parts or "CPU" in parts or "IFACE" in parts or "DEV" in parts:
            continue

        if parts[0] and parts[0][0].isdigit():
            data.append(parts)

    return data


def get_hostname(filepath):
    """Extract hostname from SAR file. Returns empty string if not found."""
    try:
        result = subprocess.run(
            ["sar", "-u", "-f", filepath, "1", "0"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[0] == "Linux":
                return parts[1]
    except Exception:
        pass
    return ""


def convert_time(times, source_tz="UTC", target_tz="UTC"):

    src = pytz.timezone(source_tz)
    tgt = pytz.timezone(target_tz)

    converted = []
    for t in times:
        s = str(t).strip()
        try:
            if " " in s and any(x in s.upper() for x in ("AM", "PM")):
                dt = datetime.strptime(s, "%I:%M:%S %p")
            else:
                dt = datetime.strptime(s, "%H:%M:%S")
            dt = src.localize(dt)
            dt = dt.astimezone(tgt)
            converted.append(dt.strftime("%H:%M:%S"))
        except Exception:
            converted.append(t)

    return converted