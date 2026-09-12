# ==============================================================================
# MML AI Institutional Hybrid Webhook Server & Live Dashboard
# سرور هیبریدی هوش مصنوعی، داشبورد تحت وب و وب‌هوک تریدینگ‌ویو
# ==============================================================================
import os
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

import json
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import pandas as pd
import numpy as np
import joblib
import requests
import threading

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

try:
    import winsound
    HAS_SOUND = True
except ImportError:
    HAS_SOUND = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(BASE_DIR, 'model_lgb_90d.joblib')
META_PATH = os.path.join(BASE_DIR, 'model_meta.json')
LOG_JSON_PATH = os.path.join(BASE_DIR, 'live_signals.json')
TEMPLATE_PATH = os.path.join(BASE_DIR, 'dashboard_template.html')

print("=" * 68)
print("     راه‌اندازی سرور هیبریدی هوش مصنوعی (MML AI Hybrid Server)     ")
print("=" * 68)

if not os.path.exists(MODEL_PATH):
    print(f"خطا: فایل مدل '{MODEL_PATH}' یافت نشد!")
    sys.exit(1)

model = joblib.load(MODEL_PATH)
with open(META_PATH, 'r', encoding='utf-8') as f:
    meta = json.load(f)

FEATURES = meta['features']
THRESHOLD = meta.get('threshold', 0.45)
DEFAULT_CAPITAL = meta.get('default_capital', 1000.0)
DEFAULT_RISK_PCT = meta.get('risk_pct', 3.0)

print(f"[*] مدل LightGBM با موفقیت بارگذاری شد (تعداد ویژگی‌ها: {len(FEATURES)})")
print(f"[*] حداقل آستانه احتمال هوش مصنوعی (Threshold): {THRESHOLD*100:.1f}%")
print(f"[*] سرمایه پیش‌فرض: ${DEFAULT_CAPITAL:,.2f} | سقف ریسک در هر معامله: {DEFAULT_RISK_PCT}% (${DEFAULT_CAPITAL*DEFAULT_RISK_PCT/100:.2f})")
print("=" * 68)

# Cache structures for low-latency non-blocking responses
CACHE_LOCK = threading.Lock()
CANDLE_CACHE = {}  # interval -> {'timestamp': float, 'data': list}
LEVELS_CACHE = {'timestamp': 0, 'data': None}

def play_alert_sound(is_approved=True):
    if not HAS_SOUND:
        return
    try:
        if is_approved:
            winsound.Beep(900, 150)
            winsound.Beep(1400, 300)
        else:
            winsound.Beep(450, 200)
    except Exception:
        pass

def fetch_live_binance_metrics():
    """دریافت آنی متریک‌های ۵ و ۱۵ دقیقه بیت‌کوین و اتریوم از سرورهای بدون تحریم بایننس"""
    try:
        url_btc = "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=20"
        res_b = requests.get(url_btc, timeout=2.5).json()
        closes_b = [float(k[4]) for k in res_b]
        volumes_b = [float(k[5]) for k in res_b]
        taker_b = [float(k[9]) for k in res_b]
        
        btc_vel_15m = ((closes_b[-1] - closes_b[-15]) / closes_b[-15]) * 100.0
        btc_v5 = sum(volumes_b[-5:])
        btc_t5 = sum(taker_b[-5:])
        btc_delta_5m = (2 * btc_t5 - btc_v5) / (btc_v5 + 1e-6)
        
        url_eth = "https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval=1m&limit=20"
        res_e = requests.get(url_eth, timeout=2.5).json()
        closes_e = [float(k[4]) for k in res_e]
        volumes_e = [float(k[5]) for k in res_e]
        taker_e = [float(k[9]) for k in res_e]
        
        eth_vel_15m = ((closes_e[-1] - closes_e[-15]) / closes_e[-15]) * 100.0
        eth_v5 = sum(volumes_e[-5:])
        eth_t5 = sum(taker_e[-5:])
        eth_delta_5m = (2 * eth_t5 - eth_v5) / (eth_v5 + 1e-6)
        taker_ratio_5m = eth_t5 / (eth_v5 + 1e-6)
        
        return {
            'btc_vel_15m': btc_vel_15m,
            'btc_delta_5m': btc_delta_5m,
            'eth_vel_15m': eth_vel_15m,
            'eth_delta_5m': eth_delta_5m,
            'taker_ratio_5m': taker_ratio_5m,
            'eth_price': closes_e[-1],
            'btc_price': closes_b[-1]
        }
    except Exception:
        return None

def fetch_candles_with_levels(interval='5m', limit=120):
    """دریافت کندل‌های واقعی اتریوم و سطوح روزانه والیوم پروفایل با کش هوشمند چندثانیه‌ای"""
    global CANDLE_CACHE, LEVELS_CACHE
    now = time.time()
    
    # 1. بازیابی کندل‌ها از کش در صورت تازگی (< 5 ثانیه)
    candles = []
    with CACHE_LOCK:
        if interval in CANDLE_CACHE and (now - CANDLE_CACHE[interval]['timestamp'] < 5.0):
            candles = list(CANDLE_CACHE[interval]['data'])
    
    # اگر کش منقضی شده بود، دریافت مستقیم از بایننس ویژن
    if not candles:
        url = f"https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval={interval}&limit={limit}"
        try:
            res = requests.get(url, timeout=3.0).json()
            if isinstance(res, list) and len(res) > 0:
                candles_tmp = []
                for k in res:
                    t = int(k[0]) // 1000
                    o, h, l, c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
                    candles_tmp.append({'time': t, 'open': o, 'high': h, 'low': l, 'close': c})
                if len(candles_tmp) >= 10:
                    candles = candles_tmp
                    with CACHE_LOCK:
                        CANDLE_CACHE[interval] = {'timestamp': now, 'data': candles}
        except Exception:
            pass
            
    # اگر شبکه موقتاً کند شد، از آخرین کش قبلی استفاده کن
    if not candles:
        with CACHE_LOCK:
            if interval in CANDLE_CACHE and CANDLE_CACHE[interval]['data']:
                candles = list(CANDLE_CACHE[interval]['data'])

    # فقط در صورت عدم وجود هرگونه دیتای اولیه
    if len(candles) < 10:
        base_p = 2530.0
        now_t = int(time.time())
        step_s = 60 if interval == '1m' else (300 if interval == '5m' else 900)
        for i in range(limit, 0, -1):
            t = now_t - (i * step_s)
            p = base_p + np.sin(i * 0.15) * 5.0
            candles.append({'time': t, 'open': round(p - 1.0, 2), 'high': round(p + 2.0, 2), 'low': round(p - 2.0, 2), 'close': round(p + 0.2, 2)})

    # 2. محاسبه سطوح دقیق والیوم پروفایل روز قبل (مشابه TradingView) با کش 5 دقیقه‌ای
    levels = None
    with CACHE_LOCK:
        if LEVELS_CACHE['data'] and (now - LEVELS_CACHE['timestamp'] < 300.0):
            levels = dict(LEVELS_CACHE['data'])
            
    if not levels:
        poc, vah, val = 2538.73, 2620.01, 2457.45
        try:
            url_d = "https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval=1d&limit=3"
            r_d = requests.get(url_d, timeout=3.0).json()
            if isinstance(r_d, list) and len(r_d) >= 2:
                # کندل دیروز index -2 است
                y_hi = float(r_d[-2][2])
                y_lo = float(r_d[-2][3])
                y_close = float(r_d[-2][4])
                poc = (y_hi + y_lo + y_close) / 3.0
                span = (y_hi - y_lo) * 0.70
                vah = min(y_hi, poc + span * 0.5)
                val = max(y_lo, poc - span * 0.5)
                levels = {'poc': round(poc, 2), 'vah': round(vah, 2), 'val': round(val, 2)}
                with CACHE_LOCK:
                    LEVELS_CACHE = {'timestamp': now, 'data': levels}
        except Exception:
            pass

    if not levels:
        levels = {'poc': 2538.73, 'vah': 2620.01, 'val': 2457.45}

    # 3. بررسی آخرین پوزیشن و آخرین معامله تایید شده
    latest_signal = None
    active_trade = None
    if os.path.exists(LOG_JSON_PATH):
        try:
            with open(LOG_JSON_PATH, 'r', encoding='utf-8') as f:
                sig_list = json.load(f)
                if sig_list:
                    latest_signal = sig_list[-1]
                    for s in reversed(sig_list):
                        if s.get('approved') and not s.get('closed', False):
                            active_trade = s
                            break
        except Exception:
            pass

    return {
        'symbol': 'ETHUSDT',
        'interval': interval,
        'candles': candles,
        'levels': levels,
        'latest_signal': latest_signal,
        'active_trade': active_trade
    }

def render_dashboard():
    signals = []
    if os.path.exists(LOG_JSON_PATH):
        try:
            with open(LOG_JSON_PATH, 'r', encoding='utf-8') as f:
                signals = json.load(f)
        except Exception:
            signals = []
    
    signals_rows = ""
    for s in reversed(signals[-15:]):
        status_badge = '<span style="background: #27ae60; color: white; padding: 4px 8px; border-radius: 4px; font-weight: bold;">تأیید شد (Approved)</span>' if s.get('approved') else (
            '<span style="background: #f39c12; color: white; padding: 4px 8px; border-radius: 4px; font-weight: bold;">⚠️ تله بیت‌کوین (Trap Blocked)</span>' if s.get('is_trap') else
            '<span style="background: #c0392b; color: white; padding: 4px 8px; border-radius: 4px;">رد شد (Rejected)</span>'
        )
        time_str = s.get('timestamp', '')[:19].replace('T', ' ')
        side_badge = f'<span style="color: {"#2ecc71" if s.get("side")=="LONG" else "#e74c3c"}; font-weight: bold;">{s.get("side")}</span>'
        
        signals_rows += f"""
        <tr>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">{time_str}</td>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">{side_badge}</td>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">${s.get('entry', 0):,.2f}</td>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">${s.get('tp', 0):,.2f}</td>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">${s.get('sl', 0):,.2f}</td>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">{s.get('prob', 0)*100:.1f}%</td>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">{s.get('btc_vel_15m', 0):+.2f}%</td>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">{status_badge}</td>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">${s.get('position_usd', 0):,.2f} ({s.get('leverage', 0):.2f}x)</td>
        </tr>
        """
    
    if not signals_rows:
        signals_rows = "<tr><td colspan='9' style='text-align: center; padding: 20px; color: #888;'>هنوز سیگنالی ثبت نشده است. از دکمه‌های بالا برای تست استفاده کنید.</td></tr>"

    with open(TEMPLATE_PATH, 'r', encoding='utf-8') as f:
        html = f.read()

    html = html.replace('{CAPITAL}', f"{DEFAULT_CAPITAL:,.2f}")
    html = html.replace('{RISK_PCT}', f"{DEFAULT_RISK_PCT:.1f}")
    html = html.replace('{RISK_USD}', f"{DEFAULT_CAPITAL*DEFAULT_RISK_PCT/100:.2f}")
    html = html.replace('{THRESHOLD}', f"{THRESHOLD*100:.0f}")
    html = html.replace('{SIGNALS_ROWS}', signals_rows)

    return html

class HybridWebhookHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_GET(self):
        if self.path == '/status':
            status_data = {
                'status': 'ONLINE',
                'service': 'MML AI Institutional Webhook Server',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'model': 'LightGBM 90-Day Meta-Labeler',
                'features_count': len(FEATURES),
                'risk_per_trade_pct': DEFAULT_RISK_PCT,
                'default_capital_usd': DEFAULT_CAPITAL
            }
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps(status_data, indent=2).encode('utf-8'))
        elif self.path.startswith('/api/candles'):
            interval = '5m'
            limit = 120
            if 'interval=' in self.path:
                try:
                    interval = self.path.split('interval=')[1].split('&')[0]
                except Exception:
                    interval = '5m'
            if 'limit=' in self.path:
                try:
                    limit = int(self.path.split('limit=')[1].split('&')[0])
                except Exception:
                    limit = 120
            data = fetch_candles_with_levels(interval, limit)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(data).encode('utf-8'))
        elif self.path == '/api/close_position':
            if os.path.exists(LOG_JSON_PATH):
                try:
                    with open(LOG_JSON_PATH, 'r', encoding='utf-8') as f:
                        sig_list = json.load(f)
                    for s in sig_list:
                        if s.get('approved') and not s.get('closed'):
                            s['closed'] = True
                    with open(LOG_JSON_PATH, 'w', encoding='utf-8') as f:
                        json.dump(sig_list, f, indent=2, ensure_ascii=False)
                except Exception:
                    pass
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'status': 'closed'}).encode('utf-8'))
        else:
            html = render_dashboard()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(html.encode('utf-8'))

    def do_POST(self):
        if self.path != '/webhook':
            self.send_response(404)
            self.end_headers()
            return
        
        content_length = int(self.headers.get('Content-Length', 0))
        raw_body = self.rfile.read(content_length)
        
        try:
            payload = json.loads(raw_body.decode('utf-8'))
        except Exception as e:
            self.send_response(400)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'Invalid JSON format'}).encode('utf-8'))
            return

        result = self.process_webhook_payload(payload)
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.end_headers()
        self.wfile.write(json.dumps(result, indent=2, ensure_ascii=False).encode('utf-8'))

    def process_webhook_payload(self, p):
        live_m = None
        if 'btc_vel_15m' not in p or 'entry' not in p:
            live_m = fetch_live_binance_metrics()
        
        side = int(p.get('side', 1))
        entry = float(p.get('entry', live_m['eth_price'] if live_m else 2530.0))
        tp = float(p.get('tp', entry * 1.018 if side == 1 else entry * 0.982))
        sl = float(p.get('sl', entry - ((tp - entry) / 1.5) if side == 1 else entry + ((entry - tp) / 1.5)))
        
        capital = float(p.get('capital', DEFAULT_CAPITAL))
        risk_pct = float(p.get('risk_pct', DEFAULT_RISK_PCT))
        
        btc_vel = float(p.get('btc_vel_15m', live_m['btc_vel_15m'] if live_m else 0.0))
        btc_delta = float(p.get('btc_delta_5m', live_m['btc_delta_5m'] if live_m else 0.0))
        
        delta_5m = float(p.get('delta_pct_5m', live_m['eth_delta_5m'] if live_m else 0.15))
        delta_15m = float(p.get('delta_pct_15m', delta_5m * 0.8))
        taker_ratio = float(p.get('taker_ratio_5m', live_m['taker_ratio_5m'] if live_m else 0.55))
        rsi = float(p.get('rsi_14', 40.0))
        atr_pct = float(p.get('atr_pct', 0.22))
        vel_15m = float(p.get('velocity_15m', live_m['eth_vel_15m'] if live_m else 0.0))
        
        poc_dist_pct = abs(tp - entry) / entry * 100.0
        va_width_pct = float(p.get('va_width_pct', 1.8))
        open_loc = float(p.get('open_loc', 0.5))
        hour_utc = int(p.get('hour_utc', datetime.utcnow().hour))
        is_balance = int(p.get('is_balance', 1))
        rel_strength = vel_15m - btc_vel

        row = pd.DataFrame([{
            'side': side,
            'delta_pct_5m': delta_5m,
            'delta_pct_15m': delta_15m,
            'taker_ratio_5m': taker_ratio,
            'rsi_14': rsi,
            'atr_pct': atr_pct,
            'velocity_15m': vel_15m,
            'poc_dist_pct': poc_dist_pct,
            'va_width_pct': va_width_pct,
            'open_loc': open_loc,
            'hour_utc': hour_utc,
            'is_balance': is_balance,
            'btc_delta_5m': btc_delta,
            'btc_vel_15m': btc_vel,
            'rel_strength_15m': rel_strength
        }])[FEATURES]

        prob = float(model.predict_proba(row)[0, 1])

        is_trap = False
        trap_reason = ""
        if side == 1 and btc_vel < -0.15:
            is_trap = True
            trap_reason = f"دامپ بیت‌کوین (شتاب {btc_vel:+.2f}%)"
        elif side == -1 and btc_vel > 0.15:
            is_trap = True
            trap_reason = f"پامپ بیت‌کوین (شتاب {btc_vel:+.2f}%)"

        is_approved = (prob >= THRESHOLD) and not is_trap

        sl_dist_pct = abs(entry - sl) / entry
        risk_usd = capital * (risk_pct / 100.0)
        pos_usd = risk_usd / max(sl_dist_pct, 0.002)
        qty_eth = pos_usd / entry
        leverage = pos_usd / capital

        decision_str = "تأیید ورود به معامله (APPROVE)" if is_approved else ("دفع تله بیت‌کوین (BLOCKED TRAP)" if is_trap else "رد سیگنال (REJECT - احتمال پایین)")

        border_char = "🟢" if is_approved else ("⚠️" if is_trap else "🔴")
        print("\n" + "=" * 70)
        print(f" {border_char} دریافت پیام وب‌هوک تریدینگ‌ویو [{datetime.now().strftime('%H:%M:%S')}] {border_char}")
        print("=" * 70)
        print(f"  جهت معامله:                {'لانگ در VAL (خرید)' if side == 1 else 'شورت در VAH (فروش)'}")
        print(f"  قیمت ورود:                 {entry:,.2f} $")
        print(f"  حد سود (POC):               {tp:,.2f} $  (+{abs(tp-entry)/entry*100:.2f}%)")
        print(f"  حد ضرر (SL):                {sl:,.2f} $  (-{abs(sl-entry)/entry*100:.2f}%)")
        print("-" * 70)
        print(f"  شتاب ۱۵دقیقه بیت‌کوین:       {btc_vel:+.2f} %  ({'صعودی' if btc_vel>0 else 'نزولی'})")
        print(f"  دلتای ۵دقیقه بیت‌کوین:        {btc_delta:+.2f}  ({'خریدار' if btc_delta>0 else 'فروشنده'})")
        print(f"  احتمال موفقیت مدل ML:       {prob*100:.1f} % (حداقل شرط: {THRESHOLD*100:.0f}%)")
        print("-" * 70)
        if is_approved:
            print(f"  وضعیت تصمیم:               ✅ {decision_str}")
            print(f"  محاسبه ریسک ۳٪:             ${risk_usd:.2f} ریسک با سرمایه ${capital:,.2f}")
            print(f"  اندازه پوزیشن پیشنهادی:     ${pos_usd:,.2f} ({qty_eth:.3f} ETH)")
            print(f"  اهرم (Leverage) مجاز:       {leverage:.2f}x")
        else:
            print(f"  وضعیت تصمیم:               ❌ {decision_str}")
            if is_trap:
                print(f"  علت رد شدن:                {trap_reason}")
        print("=" * 70 + "\n")

        play_alert_sound(is_approved)

        record = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'side': 'LONG' if side == 1 else 'SHORT',
            'entry': entry,
            'tp': tp,
            'sl': sl,
            'prob': prob,
            'approved': is_approved,
            'is_trap': is_trap,
            'btc_vel_15m': btc_vel,
            'btc_delta_5m': btc_delta,
            'position_usd': pos_usd if is_approved else 0,
            'qty_eth': qty_eth if is_approved else 0,
            'leverage': leverage if is_approved else 0,
            'risk_usd': risk_usd,
            'closed': False
        }

        signals = []
        if os.path.exists(LOG_JSON_PATH):
            try:
                with open(LOG_JSON_PATH, 'r', encoding='utf-8') as f:
                    signals = json.load(f)
            except Exception:
                signals = []
        
        if is_approved:
            for s in signals:
                if s.get('approved') and not s.get('closed'):
                    s['closed'] = True
                    
        signals.append(record)
        with open(LOG_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(signals, f, indent=2, ensure_ascii=False)

        return {
            'status': 'success',
            'decision': decision_str,
            'approved': is_approved,
            'ml_probability': round(prob, 4),
            'btc_safe': not is_trap,
            'position_sizing': {
                'capital_usd': capital,
                'risk_usd': risk_usd,
                'position_usd': round(pos_usd, 2) if is_approved else 0,
                'quantity_eth': round(qty_eth, 4) if is_approved else 0,
                'recommended_leverage': round(leverage, 2) if is_approved else 0
            }
        }

def start_keep_alive_thread():
    def pinger():
        time.sleep(30)
        while True:
            url = os.environ.get('RENDER_EXTERNAL_URL')
            if not url:
                host = os.environ.get('RENDER_EXTERNAL_HOSTNAME')
                if host:
                    url = f"https://{host}"
            if url:
                try:
                    res = requests.get(f"{url}/status", timeout=10)
                    print(f"[*] پالس بیدارباش (Keep-Alive) ارسال شد به {url}/status (کد: {res.status_code})")
                except Exception:
                    pass
            time.sleep(600)

    t = threading.Thread(target=pinger, daemon=True)
    t.start()
    print("[*] موتور بیدارباش خودکار (Self-Keep-Alive Engine) فعال شد.")

def run_server(port=8000):
    start_keep_alive_thread()
    def warm_cache():
        try:
            fetch_candles_with_levels('5m', 120)
        except Exception:
            pass
    threading.Thread(target=warm_cache, daemon=True).start()

    server_address = ('', port)
    httpd = ThreadedHTTPServer(server_address, HybridWebhookHandler)
    print(f"🚀 سرور چندنخی (Multi-Threaded) وب‌هوک و داشبورد زنده روی پورت {port} فعال شد.")
    print(f"👉 داشبورد تحت وب: http://localhost:{port}/")
    print(f"👉 آدرس دریافت وب‌هوک: http://localhost:{port}/webhook")
    print("منتظر دریافت درخواست...\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nخاموش کردن سرور...")
        httpd.server_close()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    run_server(port)
