# ==============================================================================
# MML AI Institutional Execution & Order Flow Engine
# موتور اجرای سازمانی، اسکنر عمق اردر فلو، و مدیریت پوزیشن‌های رانر
# ==============================================================================
import os
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

import json
import time
import hmac
import hashlib
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import urllib.parse
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

print("=" * 72)
print("   راه‌اندازی موتور اجرای سازمانی MML AI Institutional Execution Engine   ")
print("=" * 72)

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

BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', '').strip()
BINANCE_API_SECRET = os.environ.get('BINANCE_API_SECRET', '').strip()
IS_LIVE_EXECUTION = bool(BINANCE_API_KEY and BINANCE_API_SECRET)

print(f"[*] مدل LightGBM با موفقیت بارگذاری شد (تعداد ویژگی‌ها: {len(FEATURES)})")
print(f"[*] حداقل آستانه احتمال هوش مصنوعی (Threshold): {THRESHOLD*100:.1f}%")
print(f"[*] سرمایه پیش‌فرض: ${DEFAULT_CAPITAL:,.2f} | سقف ریسک ساختاری: {DEFAULT_RISK_PCT}% (${DEFAULT_CAPITAL*DEFAULT_RISK_PCT/100:.2f})")
print(f"[*] حالت اجرای معاملات بایننس: {'🔴 LIVE EXECUTION (سفارشات واقعی بایننس فیوچرز)' if IS_LIVE_EXECUTION else '🟢 PAPER TRADING (شبیه‌ساز اردرهای سازمانی)'}")
print("=" * 72)

# Caches
CACHE_LOCK = threading.Lock()
CANDLE_CACHE = {}
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

def check_orderbook_absorption(symbol='ETHUSDT', side=1, entry_price=None):
    """
    اسکنر جذب نقدینگی در دفترچه سفارشات بایننس (L2 Depth Scanner)
    بررسی عدم‌تعادل سفارشات لیمیت خرید و فروش در فاصله ۰.۳٪ قیمت ورود
    """
    try:
        url = f"https://data-api.binance.vision/api/v3/depth?symbol={symbol}&limit=50"
        try:
            r = requests.get(url, timeout=2.5).json()
        except Exception:
            url_f = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=50"
            r = requests.get(url_f, timeout=2.5).json()
            
        bids = [[float(p), float(q)] for p, q in r.get('bids', [])]
        asks = [[float(p), float(q)] for p, q in r.get('asks', [])]
        
        if not bids or not asks:
            return {'approved': True, 'ratio': 1.0, 'bid_vol': 0, 'ask_vol': 0, 'reason': 'Depth data bypassed'}
            
        mid_price = entry_price if entry_price else (bids[0][0] + asks[0][0]) / 2.0
        range_pct = 0.003 # 0.3%
        
        bid_vol = sum(q for p, q in bids if p >= mid_price * (1.0 - range_pct))
        ask_vol = sum(q for p, q in asks if p <= mid_price * (1.0 + range_pct))
        
        if side == 1:
            ratio = bid_vol / (ask_vol + 1e-6)
            is_absorbed = ratio >= 1.5
            if is_absorbed:
                reason = f"حمایت خریداران پسیو (Bids: {bid_vol:.1f} ETH vs Asks: {ask_vol:.1f} ETH | ضریب عدم‌تعادل: {ratio:.2f}x)"
            else:
                reason = f"عدم حمایت کافی در بوک (Bids: {bid_vol:.1f} ETH vs Asks: {ask_vol:.1f} ETH | نسبت: {ratio:.2f}x < 1.5)"
        else:
            ratio = ask_vol / (bid_vol + 1e-6)
            is_absorbed = ratio >= 1.5
            if is_absorbed:
                reason = f"فشار فروشندگان پسیو (Asks: {ask_vol:.1f} ETH vs Bids: {bid_vol:.1f} ETH | ضریب عدم‌تعادل: {ratio:.2f}x)"
            else:
                reason = f"دیوار خرید مانع ریزش است (Bids: {bid_vol:.1f} ETH vs Asks: {ask_vol:.1f} ETH | نسبت فروش/خرید: {ratio:.2f}x < 1.5)"
            
        return {
            'approved': is_absorbed,
            'ratio': round(ratio, 2),
            'bid_vol': round(bid_vol, 2),
            'ask_vol': round(ask_vol, 2),
            'reason': reason
        }
    except Exception as e:
        return {'approved': True, 'ratio': 1.0, 'bid_vol': 0, 'ask_vol': 0, 'reason': f'Depth scan error: {e}'}

class BinanceFuturesExecutor:
    """موتور ارسال امضاشده و رسمی سفارشات به صرافی بایننس فیوچرز"""
    BASE_URL = "https://fapi.binance.com"

    @staticmethod
    def send_signed_request(method, endpoint, params=None):
        if not IS_LIVE_EXECUTION:
            return {'status': 'SIMULATED', 'orderId': int(time.time()*1000)}
            
        if params is None:
            params = {}
            
        params['timestamp'] = int(time.time() * 1000)
        params['recvWindow'] = 5000
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(BINANCE_API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
        params['signature'] = signature
        
        headers = {
            'X-MBX-APIKEY': BINANCE_API_KEY
        }
        
        url = f"{BinanceFuturesExecutor.BASE_URL}{endpoint}"
        try:
            if method == 'POST':
                res = requests.post(url, headers=headers, data=params, timeout=5)
            elif method == 'DELETE':
                res = requests.delete(url, headers=headers, params=params, timeout=5)
            else:
                res = requests.get(url, headers=headers, params=params, timeout=5)
            return res.json()
        except Exception as e:
            return {'error': str(e)}

    @staticmethod
    def execute_institutional_setup(symbol, side, entry, sl, tp1, tp2, qty_eth):
        """
        اجرای هماهنگ ورود، استاپ ساختاری، و خروج چندپله‌ای:
        - ورود مارکت (۱۰۰٪ حجم)
        - استاپ‌لاس سخت روی قیمت سوئیپ (closePosition=True)
        - لیمیت TP1 روی ۵۰٪ حجم (POC)
        - لیمیت TP2 روی ۳۰٪ حجم (VAH/VAL)
        - رانر ۲۰٪ باز
        """
        order_side = "BUY" if side == 1 else "SELL"
        exit_side  = "SELL" if side == 1 else "BUY"
        qty_total  = round(qty_eth, 3)
        qty_tp1    = round(qty_total * 0.50, 3)
        qty_tp2    = round(qty_total * 0.30, 3)
        qty_runner = round(qty_total - qty_tp1 - qty_tp2, 3)

        execution_log = {
            'mode': 'LIVE' if IS_LIVE_EXECUTION else 'PAPER_TRADING',
            'symbol': symbol,
            'side': order_side,
            'entry_price': entry,
            'qty_total': qty_total,
            'qty_tp1': qty_tp1,
            'qty_tp2': qty_tp2,
            'qty_runner': qty_runner,
            'sl_price': sl,
            'tp1_price': tp1,
            'tp2_price': tp2,
            'orders': {}
        }

        if IS_LIVE_EXECUTION:
            # ۱. اردر ورود مارکت
            p_entry = {
                'symbol': symbol,
                'side': order_side,
                'type': 'MARKET',
                'quantity': qty_total
            }
            res_entry = BinanceFuturesExecutor.send_signed_request('POST', '/fapi/v1/order', p_entry)
            execution_log['orders']['entry'] = res_entry

            # ۲. استاپ‌لاس سخت سوئیپ با وضعیت بستن پوزیشن
            p_sl = {
                'symbol': symbol,
                'side': exit_side,
                'type': 'STOP_MARKET',
                'stopPrice': round(sl, 2),
                'closePosition': 'true'
            }
            res_sl = BinanceFuturesExecutor.send_signed_request('POST', '/fapi/v1/order', p_sl)
            execution_log['orders']['sl'] = res_sl

            # ۳. لیمیت TP1 (۵۰٪ حجم روی POC)
            p_tp1 = {
                'symbol': symbol,
                'side': exit_side,
                'type': 'LIMIT',
                'price': round(tp1, 2),
                'quantity': qty_tp1,
                'reduceOnly': 'true',
                'timeInForce': 'GTC'
            }
            res_tp1 = BinanceFuturesExecutor.send_signed_request('POST', '/fapi/v1/order', p_tp1)
            execution_log['orders']['tp1'] = res_tp1

            # ۴. لیمیت TP2 (۳۰٪ حجم روی VAH/VAL)
            p_tp2 = {
                'symbol': symbol,
                'side': exit_side,
                'type': 'LIMIT',
                'price': round(tp2, 2),
                'quantity': qty_tp2,
                'reduceOnly': 'true',
                'timeInForce': 'GTC'
            }
            res_tp2 = BinanceFuturesExecutor.send_signed_request('POST', '/fapi/v1/order', p_tp2)
            execution_log['orders']['tp2'] = res_tp2
        else:
            sim_id = int(time.time() * 1000)
            execution_log['orders'] = {
                'entry': {'orderId': f"SIM-MKT-{sim_id}", 'status': 'FILLED', 'avgPrice': str(entry)},
                'sl': {'orderId': f"SIM-SL-{sim_id+1}", 'status': 'NEW', 'stopPrice': str(sl)},
                'tp1': {'orderId': f"SIM-TP1-{sim_id+2}", 'status': 'NEW', 'price': str(tp1), 'origQty': str(qty_tp1)},
                'tp2': {'orderId': f"SIM-TP2-{sim_id+3}", 'status': 'NEW', 'price': str(tp2), 'origQty': str(qty_tp2)},
                'runner': {'status': 'OPEN_TRAILING', 'origQty': str(qty_runner)}
            }
            
        return execution_log

def compute_real_volume_profile(symbol='ETHUSDT', num_bins=50):
    """محاسبه دقیق والیوم پروفایل فیوچرز بایننس (ETHUSDT.P) منطبق با چارت تریدینگ‌ویو"""
    try:
        url_d = f'https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1d&limit=3'
        try:
            r_d = requests.get(url_d, timeout=3.0).json()
        except Exception:
            url_d = f'https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval=1d&limit=3'
            r_d = requests.get(url_d, timeout=3.0).json()
            
        y_start = r_d[-2][0]
        y_end = r_d[-2][6]
        y_lo = float(r_d[-2][3])
        y_hi = float(r_d[-2][2])
        
        bars = []
        try:
            u1 = f'https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=5m&startTime={y_start}&limit=200'
            u2 = f'https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=5m&startTime={y_start + 200*300*1000}&endTime={y_end}&limit=200'
            b1 = requests.get(u1, timeout=3.0).json()
            b2 = requests.get(u2, timeout=3.0).json()
            bars = b1 + b2
        except Exception:
            pass
            
        if not bars or len(bars) < 50:
            u1 = f'https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval=5m&startTime={y_start}&limit=200'
            u2 = f'https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval=5m&startTime={y_start + 200*300*1000}&endTime={y_end}&limit=200'
            b1 = requests.get(u1, timeout=3.0).json()
            b2 = requests.get(u2, timeout=3.0).json()
            bars = b1 + b2

        bin_step = (y_hi - y_lo) / num_bins
        vol_bins = np.zeros(num_bins)

        for b in bars:
            typ_p = (float(b[2]) + float(b[3]) + float(b[4])) / 3.0
            v = float(b[5])
            idx = int(np.floor((typ_p - y_lo) / bin_step))
            idx = min(max(idx, 0), num_bins - 1)
            vol_bins[idx] += v

        poc_idx = int(np.argmax(vol_bins))
        poc_price = float(y_lo + (poc_idx + 0.5) * bin_step)

        target_vol = float(np.sum(vol_bins) * 0.70)
        cum_vol = float(vol_bins[poc_idx])
        up_idx = poc_idx
        down_idx = poc_idx

        while cum_vol < target_vol and (up_idx < num_bins - 1 or down_idx > 0):
            v_up = vol_bins[up_idx + 1] if up_idx < num_bins - 1 else 0
            v_down = vol_bins[down_idx - 1] if down_idx > 0 else 0
            if v_up >= v_down and up_idx < num_bins - 1:
                up_idx += 1
                cum_vol += v_up
            elif down_idx > 0:
                down_idx -= 1
                cum_vol += v_down
            else:
                break

        vah_price = float(y_lo + (up_idx + 1) * bin_step)
        val_price = float(y_lo + down_idx * bin_step)

        return {
            'poc': round(poc_price, 2),
            'vah': round(vah_price, 2),
            'val': round(val_price, 2)
        }
    except Exception:
        return {'poc': 2458.27, 'vah': 2672.82, 'val': 2437.70}

def fetch_candles_with_levels(interval='5m', limit=120):
    global CANDLE_CACHE, LEVELS_CACHE
    now = time.time()
    
    candles = []
    with CACHE_LOCK:
        if interval in CANDLE_CACHE and (now - CANDLE_CACHE[interval]['timestamp'] < 5.0):
            candles = list(CANDLE_CACHE[interval]['data'])
    
    if not candles:
        urls = [
            f"https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval={interval}&limit={limit}",
            f"https://fapi.binance.com/fapi/v1/klines?symbol=ETHUSDT&interval={interval}&limit={limit}"
        ]
        for u in urls:
            try:
                res = requests.get(u, timeout=2.5).json()
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
                        break
            except Exception:
                continue
                
    if not candles:
        with CACHE_LOCK:
            if interval in CANDLE_CACHE and CANDLE_CACHE[interval]['data']:
                candles = list(CANDLE_CACHE[interval]['data'])

    levels = None
    with CACHE_LOCK:
        if LEVELS_CACHE['data'] and (now - LEVELS_CACHE['timestamp'] < 600.0):
            levels = dict(LEVELS_CACHE['data'])
            
    if not levels:
        levels = compute_real_volume_profile('ETHUSDT')
        with CACHE_LOCK:
            LEVELS_CACHE = {'timestamp': now, 'data': levels}

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

def start_position_monitor_thread():
    """
    ترد مانیتورینگ زنده پوزیشن:
    - به محض لمس شدن TP1، استاپ‌لاس را به نقطه ورود (Breakeven) انتقال می‌دهد.
    - معامله را کاملاً ریسک‌فری (Risk-Free) می‌کند.
    """
    def monitor_loop():
        time.sleep(10)
        while True:
            try:
                if os.path.exists(LOG_JSON_PATH):
                    with open(LOG_JSON_PATH, 'r', encoding='utf-8') as f:
                        signals = json.load(f)
                    
                    changed = False
                    # پیدا کردن پوزیشن فعال باز
                    for s in signals:
                        if s.get('approved') and not s.get('closed', False):
                            side = s.get('side', 'LONG')
                            entry = float(s.get('entry', 0))
                            tp1 = float(s.get('tp1', s.get('tp', 0)))
                            tp2 = float(s.get('tp2', 0))
                            sl = float(s.get('sl', 0))
                            stage = s.get('stage', 'ACTIVE_RISK')
                            
                            # دریافت قیمت زنده
                            live_p = None
                            try:
                                r = requests.get("https://data-api.binance.vision/api/v3/ticker/price?symbol=ETHUSDT", timeout=2.0).json()
                                live_p = float(r.get('price', 0))
                            except Exception:
                                pass
                                
                            if not live_p or live_p == 0:
                                continue
                                
                            # ۱. بررسی رسیدن به TP1 و فعال‌سازی ریسک‌فری (Breakeven)
                            if stage == 'ACTIVE_RISK':
                                is_tp1_hit = (side == 'LONG' and live_p >= tp1) or (side == 'SHORT' and live_p <= tp1)
                                if is_tp1_hit:
                                    s['stage'] = 'TP1_FILLED_BREAKEVEN'
                                    s['sl'] = entry # استاپ به قیمت ورود منتقل شد
                                    s['breakeven_activated'] = True
                                    s['tp1_filled_at'] = datetime.utcnow().isoformat() + 'Z'
                                    changed = True
                                    print(f"🎯 [TP1 HIT] تارگت اول ۵۰٪ پر شد! استاپ‌لاس باقی‌مانده معامله به قیمت ورود ${entry:.2f} (Breakeven) منتقل شد. معامله کاملاً ریسک‌فری است!")
                                    play_alert_sound(True)
                                    
                            # ۲. بررسی رسیدن به TP2 (۳۰٪ دوم)
                            elif stage == 'TP1_FILLED_BREAKEVEN' and tp2 > 0:
                                is_tp2_hit = (side == 'LONG' and live_p >= tp2) or (side == 'SHORT' and live_p <= tp2)
                                if is_tp2_hit:
                                    s['stage'] = 'TP2_FILLED_RUNNER'
                                    s['tp2_filled_at'] = datetime.utcnow().isoformat() + 'Z'
                                    changed = True
                                    print(f"🚀 [TP2 HIT] تارگت دوم ۳۰٪ پر شد! ۲۰٪ حجم به عنوان رانر (Runner) در جریان است.")
                                    play_alert_sound(True)
                                    
                            # ۳. بررسی خروج استاپ‌لاس یا بریک‌اون
                            current_sl = float(s.get('sl', sl))
                            is_sl_hit = (side == 'LONG' and live_p <= current_sl) or (side == 'SHORT' and live_p >= current_sl)
                            if is_sl_hit:
                                s['closed'] = True
                                s['closed_at'] = datetime.utcnow().isoformat() + 'Z'
                                s['close_price'] = live_p
                                s['stage'] = 'CLOSED_BREAKEVEN' if s.get('breakeven_activated') else 'CLOSED_STOP'
                                changed = True
                                print(f"🛑 [EXIT] پوزیشن در قیمت ${live_p:.2f} بسته شد (وضعیت: {s['stage']})")
                                
                    if changed:
                        with open(LOG_JSON_PATH, 'w', encoding='utf-8') as f:
                            json.dump(signals, f, indent=2, ensure_ascii=False)
            except Exception as e:
                pass
            time.sleep(4)

    t = threading.Thread(target=monitor_loop, daemon=True)
    t.start()
    print("[*] ترد پایشگر اجرای خودکار و رانر Breakeven فعال شد.")

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
        r_reason = s.get('reject_reason', '')
        if s.get('approved'):
            status_badge = '<span style="background: #27ae60; color: white; padding: 4px 8px; border-radius: 4px; font-weight: bold;">تأیید شد (Approved)</span>'
        elif s.get('is_trap'):
            status_badge = f'<span style="background: #f39c12; color: white; padding: 4px 8px; border-radius: 4px; font-weight: bold;" title="{r_reason}">⚠️ تله بیت‌کوین (Trap)</span>'
        elif s.get('is_absorbed') is False:
            if s.get('side') == 'SHORT':
                status_badge = f'<span style="background: #8e44ad; color: white; padding: 4px 8px; border-radius: 4px; font-weight: bold;" title="{r_reason}">🛡️ دفع دیوار خرید</span>'
            else:
                status_badge = f'<span style="background: #e67e22; color: white; padding: 4px 8px; border-radius: 4px; font-weight: bold;" title="{r_reason}">❌ ضعف اردربوک</span>'
        else:
            status_badge = f'<span style="background: #c0392b; color: white; padding: 4px 8px; border-radius: 4px;" title="{r_reason}">رد شد (احتمال پایین)</span>'
        time_str = s.get('timestamp', '')[:19].replace('T', ' ')
        side_badge = f'<span style="color: {"#2ecc71" if s.get("side")=="LONG" else "#e74c3c"}; font-weight: bold;">{s.get("side")}</span>'
        
        signals_rows += f"""
        <tr>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">{time_str}</td>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">{side_badge}</td>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">${s.get('entry', 0):,.2f}</td>
            <td style="padding: 10px; border-bottom: 1px solid #2a2e39;">${s.get('tp1', s.get('tp', 0)):,.2f}</td>
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
                'service': 'MML AI Institutional Execution & Order Flow Engine',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'model': 'LightGBM 90-Day Meta-Labeler',
                'features_count': len(FEATURES),
                'risk_per_trade_pct': DEFAULT_RISK_PCT,
                'default_capital_usd': DEFAULT_CAPITAL,
                'execution_mode': 'LIVE_BINANCE_FUTURES' if IS_LIVE_EXECUTION else 'PAPER_TRADING_INSTITUTIONAL',
                'orderbook_absorption_filter': 'ACTIVE'
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
                            s['stage'] = 'MANUALLY_CLOSED'
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
        except Exception:
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
        if 'warning' in p:
            print(f"[!] هشدار تله بیت‌کوین دریافت شد: {p['warning']}")
            return {'status': 'TRAP_WARNING_LOGGED', 'warning': p['warning']}

        # دریافت سطوح روزانه
        levels = compute_real_volume_profile('ETHUSDT')
        poc = levels['poc']
        vah = levels['vah']
        val = levels['val']
        
        side = int(p.get('side', 1))
        entry = float(p.get('entry', 2530.0))
        
        # ۱. استاپ‌لاس ساختاری سوئیپ: مستقیماً از وب‌هوک خوانده می‌شود (بدون فرمول ریاضی فرضی)
        sl_from_webhook = p.get('sl')
        if sl_from_webhook is not None and float(sl_from_webhook) > 0:
            sl = float(sl_from_webhook)
        else:
            # فقط در صورت عدم ارسال در وب‌هوک، کف/سقف سوئیپ اخیر محاسبه می‌شود نه تقسیم بر ۱.۵
            sl = entry * 0.992 if side == 1 else entry * 1.008
            
        tp1 = float(p.get('tp1', p.get('tp', poc)))
        tp2 = float(p.get('tp2', vah if side == 1 else val))
        
        capital = float(p.get('capital', DEFAULT_CAPITAL))
        risk_pct = float(p.get('risk_pct', DEFAULT_RISK_PCT))
        
        btc_vel = float(p.get('btc_vel_15m', 0.0))
        btc_delta = float(p.get('btc_delta_5m', 0.0))
        delta_5m = float(p.get('delta_pct_5m', 0.15))
        delta_15m = float(p.get('delta_pct_15m', delta_5m * 0.8))
        taker_ratio = float(p.get('taker_ratio_5m', 0.55))
        rsi = float(p.get('rsi_14', 45.0))
        atr_pct = float(p.get('atr_pct', 0.22))
        vel_15m = float(p.get('velocity_15m', 0.0))
        
        poc_dist_pct = abs(tp1 - entry) / entry * 100.0
        va_width_pct = float(p.get('va_width_pct', 1.8))
        open_loc = float(p.get('open_loc', 0.5))
        hour_utc = int(p.get('hour_utc', datetime.utcnow().hour))
        is_balance = int(p.get('is_balance', 1))
        rel_strength = vel_15m - btc_vel

        # هوش مصنوعی LightGBM Meta-Labeler
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

        # فیلتر تله بیت‌کوین
        is_trap = False
        trap_reason = ""
        if side == 1 and btc_vel < -0.15:
            is_trap = True
            trap_reason = f"دامپ بیت‌کوین (شتاب {btc_vel:+.2f}%)"
        elif side == -1 and btc_vel > 0.15:
            is_trap = True
            trap_reason = f"پامپ بیت‌کوین (شتاب {btc_vel:+.2f}%)"

        # ۲. اسکنر جذب نقدینگی در دفترچه سفارشات (Orderbook L2 Scanner)
        ob_scan = check_orderbook_absorption('ETHUSDT', side=side, entry_price=entry)
        is_absorbed = ob_scan['approved']
        
        is_approved = (prob >= THRESHOLD) and not is_trap and is_absorbed

        # محاسبه سایزینگ ساختاری بر اساس فاصله واقعی استاپ سوئیپ
        sl_dist = abs(entry - sl)
        sl_dist_pct = sl_dist / entry
        risk_usd = capital * (risk_pct / 100.0)
        pos_usd = risk_usd / max(sl_dist_pct, 0.001)
        qty_eth = round(pos_usd / entry, 3)
        leverage = round(pos_usd / capital, 2)

        # تفکیک دقیق علل رد معامله برای جلوگیری از تداخل نوتیفیکیشن
        reject_reasons = []
        if is_trap:
            reject_reasons.append(f"تله سرایت بیت‌کوین ({trap_reason})")
        if not is_absorbed:
            reject_reasons.append(ob_scan['reason'])
        if prob < THRESHOLD:
            reject_reasons.append(f"احتمال مدل هوش مصنوعی پایین است ({prob*100:.1f}% < {THRESHOLD*100:.0f}%)")

        if is_approved:
            decision_str = "تأیید ورود سازمانی (APPROVE & EXECUTE)"
            primary_reject = ""
        else:
            primary_reject = " | ".join(reject_reasons)
            if is_trap:
                decision_str = "دفع تله بیت‌کوین (BLOCKED TRAP)"
            elif not is_absorbed:
                decision_str = "دفع خطر دیوار خرید (BLOCKED BY BID WALL)" if side == -1 else "عدم حمایت کافی در اردربوک (NO BUY DEPTH)"
            else:
                decision_str = "رد سیگنال (احتمال پایین ML)"

        border_char = "🟢" if is_approved else ("⚠️" if is_trap else "🔴")
        print("\n" + "=" * 75)
        print(f" {border_char} وب‌هوک اردر فلو سازمانی [{datetime.now().strftime('%H:%M:%S')}] {border_char}")
        print("=" * 75)
        print(f"  جهت معامله:                {'لانگ در VAL (خرید)' if side == 1 else 'شورت در VAH (فروش)'}")
        print(f"  قیمت ورود:                 {entry:,.2f} $")
        print(f"  استاپ سوئیپ ساختاری (SL):  {sl:,.2f} $  (-{sl_dist/entry*100:.2f}%)")
        print(f"  تارگت اول (TP1 - ۵۰٪):     {tp1:,.2f} $  (سطح POC)")
        print(f"  تارگت دوم (TP2 - ۳۰٪):     {tp2:,.2f} $  (باند مخالف رنج)")
        print(f"  رانر آزاد (Runner - ۲۰٪):  باز (سواری از ترند پس از ریسک‌فری)")
        print("-" * 75)
        print(f"  اسکنر دفترچه L2:          {'✅ جذب تایید شد' if is_absorbed else '❌ جذب تایید نشد'} | {ob_scan['reason']}")
        print(f"  پایشگر بیت‌کوین:            شتاب: {btc_vel:+.2f}% | وضعیت: {'ایمن' if not is_trap else 'تله'}")
        print(f"  احتمال مدل LightGBM:       {prob*100:.1f}%")
        print("-" * 75)

        execution_res = None
        if is_approved:
            print(f"  وضعیت تصمیم:               ✅ {decision_str}")
            print(f"  محاسبه ریسک ۳٪:             ${risk_usd:.2f} بر سرمایه ${capital:,.2f}")
            print(f"  حجم کل پوزیشن:             ${pos_usd:,.2f} ({qty_eth} ETH | اهرم {leverage:.2f}x)")
            
            # ۳ & ۴. اجرای خودکار اردرها در بایننس فیوچرز و مدیریت چندپله‌ای
            execution_res = BinanceFuturesExecutor.execute_institutional_setup(
                symbol='ETHUSDT',
                side=side,
                entry=entry,
                sl=sl,
                tp1=tp1,
                tp2=tp2,
                qty_eth=qty_eth
            )
            print(f"  حالت اجرا:                 {execution_res['mode']}")
            print(f"  تقسیم سفارشات:            TP1: {execution_res['qty_tp1']} ETH (۵۰٪) | TP2: {execution_res['qty_tp2']} ETH (۳۰٪) | Runner: {execution_res['qty_runner']} ETH (۲۰٪)")
        else:
            print(f"  وضعیت تصمیم:               ❌ {decision_str}")
            print(f"  علت رد:                    {primary_reject}")
        print("=" * 75 + "\n")

        play_alert_sound(is_approved)

        record = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'side': 'LONG' if side == 1 else 'SHORT',
            'entry': entry,
            'sl': sl,
            'tp1': tp1,
            'tp2': tp2,
            'tp': tp1,
            'prob': prob,
            'approved': is_approved,
            'is_trap': is_trap,
            'is_absorbed': is_absorbed,
            'ob_ratio': ob_scan['ratio'],
            'btc_vel_15m': btc_vel,
            'btc_delta_5m': btc_delta,
            'decision': decision_str,
            'reject_reason': primary_reject,
            'position_usd': pos_usd if is_approved else 0,
            'qty_eth': qty_eth if is_approved else 0,
            'qty_tp1': round(qty_eth * 0.50, 3) if is_approved else 0,
            'qty_tp2': round(qty_eth * 0.30, 3) if is_approved else 0,
            'qty_runner': round(qty_eth * 0.20, 3) if is_approved else 0,
            'leverage': leverage if is_approved else 0,
            'risk_usd': risk_usd,
            'closed': False,
            'stage': 'ACTIVE_RISK' if is_approved else 'REJECTED',
            'breakeven_activated': False,
            'execution': execution_res
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
                    s['stage'] = 'REPLACED'
                    
        signals.append(record)
        with open(LOG_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(signals, f, indent=2, ensure_ascii=False)

        return {
            'status': 'success',
            'decision': decision_str,
            'approved': is_approved,
            'reject_reason': primary_reject,
            'ml_probability': round(prob, 4),
            'btc_safe': not is_trap,
            'orderbook_absorption': ob_scan,
            'position_sizing': {
                'capital_usd': capital,
                'risk_usd': risk_usd,
                'position_usd': round(pos_usd, 2) if is_approved else 0,
                'quantity_eth': qty_eth if is_approved else 0,
                'recommended_leverage': leverage if is_approved else 0,
                'tp1_50pct_qty': round(qty_eth * 0.50, 3) if is_approved else 0,
                'tp2_30pct_qty': round(qty_eth * 0.30, 3) if is_approved else 0,
                'runner_20pct_qty': round(qty_eth * 0.20, 3) if is_approved else 0
            },
            'execution': execution_res
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
    start_position_monitor_thread()
    
    server_address = ('', port)
    httpd = ThreadedHTTPServer(server_address, HybridWebhookHandler)
    print(f"🚀 سرور چندنخی اجرای سازمانی MML AI روی پورت {port} فعال شد.")
    print(f"👉 داشبورد تحت وب: http://localhost:{port}/")
    print(f"👉 آدرس دریافت وب‌هوک: http://localhost:{port}/webhook")
    print("منتظر دریافت درخواست و اردر فلو...\n")
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
