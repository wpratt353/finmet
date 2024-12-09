import yfinance as yf
import os
import pandas as pd
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
import time
from typing import List, Dict, Any, Tuple
import sys
from requests.exceptions import HTTPError

class RateLimitException(Exception):
    """Custom exception for rate limit handling"""
    pass

class FinancialMetricsUpdater:
    def __init__(self, spreadsheet_id: str, creds_path: str):
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=scope)
        self.service = build('sheets', 'v4', credentials=creds)
        self.spreadsheet_id = spreadsheet_id
        
        # Response codes that indicate temporary failures (don't blacklist)
        self.temporary_error_codes = {
            429,  # Rate Limit - don't blacklist
            500,  # Internal Server Error
            502,  # Bad Gateway
            503,  # Service Unavailable
            504   # Gateway Timeout
        }

    def get_stocks_to_update(self) -> List[Dict[str, Any]]:
        """Get all stocks that need updates in one batch read"""
        result = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range='Financial Metrics!A:P'
        ).execute()
        metrics_data = result.get('values', [])
        
        one_day_ago = datetime.now() - timedelta(days=1)
        candidates = []
        
        for idx, row in enumerate(metrics_data[1:], start=2):
            if len(row) < 7:
                continue
                
            try:
                active = row[1].lower() == 'true'
                last_updated = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S') if row[2] and row[2].strip() else None
                failures = int(row[4]) if row[4] and row[4].strip() else 0
                status = row[6] if len(row) > 6 and row[6] else 'pending'
                
                if (active and failures < 5 and 
                    (status == 'pending' or not last_updated or last_updated < one_day_ago)):
                    candidates.append({
                        'ticker': row[0],
                        'rowIndex': idx,
                        'failures': failures
                    })
            except (ValueError, IndexError):
                continue
                
        return sorted(candidates, key=lambda x: x['failures'])
    
    def should_blacklist(self, error: Exception) -> bool:
        """Determine if an error should trigger blacklisting"""
        # First check for rate limits since that's special
        if self._check_rate_limit(error):
            return False
            
        # If it's an HTTP error, check if it's temporary
        if hasattr(error, 'status_code'):
            return error.status_code not in self.temporary_error_codes
            
        # For any other error type, it should be blacklisted
        return True

    def update_blacklist_sheet(self, blacklist_updates: List[Dict[str, Any]]):
        """Add blacklisted stocks to a separate blacklist tracking sheet"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Get existing blacklist
        result = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range='Blacklist!A:D'
        ).execute()
        existing_data = result.get('values', [])
        next_row = len(existing_data) + 1
        
        # Prepare new entries
        data = []
        for update in blacklist_updates:
            data.append({
                'range': f'Blacklist!A{next_row}:D{next_row}',
                'values': [[
                    update.get('ticker', ''),     # Ticker
                    timestamp,                     # Blacklist Date
                    str(update.get('error', '')), # Error Message
                    update.get('failures', 0)      # Failure Count
                ]]
            })
            next_row += 1
        
        if data:
            body = {
                'valueInputOption': 'USER_ENTERED',
                'data': data
            }
            self.service.spreadsheets().values().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body
            ).execute()

    def update_metrics_batch(self, updates: List[Dict[str, Any]]):
        """Batch update Google Sheets with new metrics"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        data = []
        for update in updates:
            row_idx = update['rowIndex']
            metrics = update['metrics']
            
            data.append({
                'range': f'Financial Metrics!C{row_idx}:R{row_idx}',
                'values': [[
                    timestamp,                # Last Updated
                    timestamp,                # Last Attempt
                    0,                        # Failures
                    '',                       # Error
                    'complete',               # Status
                    metrics['fcf_yield'],
                    metrics['roe'],
                    metrics['pb'],
                    metrics['current_ratio'],
                    metrics['debt_equity'],
                    metrics['net_margin'],
                    metrics['roa'],
                    metrics['revenue_growth'],
                    metrics['ev_ebitda'],
                    metrics['quick_ratio'],
                    metrics['fair_value']
                ]]
            })
        
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': data
        }
        
        self.service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=body
        ).execute()

    def update_failures_batch(self, failed_updates: List[Dict[str, Any]]):
        """Batch update failures with specific error messages"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        data = []
        for update in failed_updates:
            row_idx = update['rowIndex']
            failures = update['failures'] + 1
            error_msg = update.get('error', 'Failed to fetch metrics')
            
            data.append({
                'range': f'Financial Metrics!C{row_idx}:G{row_idx}',
                'values': [[
                    None,           # Last Updated
                    timestamp,      # Last Attempt
                    failures,       # Failures
                    error_msg,      # Error
                    'failed'        # Status
                ]]
            })
            
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': data
        }
        
        self.service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=body
        ).execute()

    def update_active_status(self, blacklist_updates: List[Dict[str, Any]]):
        """Batch update to set active status to False for blacklisted stocks"""
        data = []
        for update in blacklist_updates:
            row_idx = update['rowIndex']
            data.append({
                'range': f'Financial Metrics!B{row_idx}',
                'values': [['FALSE']]  # Set active status to False
            })
        
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': data
        }
        
        self.service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=body
        ).execute()

    def _check_rate_limit(self, error: Exception) -> bool:
        """Check if an error is a rate limit error"""
        if isinstance(error, HTTPError):
            if error.response is not None and error.response.status_code == 429:
                return True
        if '429' in str(error):
            return True
        return False

    def get_metrics_batch(self, tickers: List[str]) -> Tuple[Dict[str, Dict], Dict[str, Dict], bool]:
        successful_results = {}
        failed_results = {}
        
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                try:
                    info = stock.info
                except HTTPError as e:
                    if e.response is not None and e.response.status_code == 429:
                        print(f"Rate limit hit on {ticker}")
                        # Return immediately with what we have so far
                        return successful_results, failed_results, True
                    raise
                
                # Validation that will trigger blacklisting
                quote_type = info.get('quoteType')
                trading_currency = info.get('currency')
                financial_currency = info.get('financialCurrency')
                country = info.get('country')
                
                # If conditions aren't met, raise an exception that will trigger blacklisting
                if quote_type != 'EQUITY':
                    raise Exception(f"Not an equity security. Quote type: {quote_type}")
                
                if trading_currency != 'USD':
                    raise Exception(f"Trading currency is not USD: {trading_currency}")
                
                if financial_currency is not None and financial_currency != 'USD':
                    raise Exception(f"Financial reporting currency is not USD: {financial_currency}")
                
                if country not in ['US', 'USA', 'United States']:
                    raise Exception(f"Company not based in US: {country}")
                                
                fcf = info.get('freeCashflow')
                market_cap = info.get('marketCap')
                
                current_price = (
                    info.get('currentPrice') or 
                    info.get('regularMarketPrice') or
                    info.get('previousClose') or 
                    info.get('open') or 
                    info.get('fiftyDayAverage')
                )
                
                metrics = {}
                metrics['fcf_yield'] = (fcf / market_cap) if (fcf and market_cap and market_cap != 0) else None
                metrics['roe'] = info.get('returnOnEquity')
                metrics['pb'] = info.get('priceToBook')
                metrics['current_ratio'] = info.get('currentRatio')
                metrics['debt_equity'] = info.get('debtToEquity')
                metrics['net_margin'] = info.get('profitMargins')
                metrics['roa'] = info.get('returnOnAssets')
                metrics['revenue_growth'] = info.get('revenueGrowth')
                metrics['ev_ebitda'] = info.get('enterpriseToEbitda')
                metrics['quick_ratio'] = info.get('quickRatio')
        
                fair_value = None
                evebitda = info.get('enterpriseToEbitda')
                peg = info.get('trailingPegRatio')
                
                if all(v is not None and v != 0 for v in [current_price, evebitda]):
                    evebitda_implied = current_price * (15 / evebitda)
                    if peg and peg != 0:
                        peg_implied = current_price * (2.0 / peg)
                        fair_value = (evebitda_implied * 0.6) + (peg_implied * 0.4)
                    else:
                        fair_value = evebitda_implied
                metrics['fair_value'] = fair_value
                        
                required_metrics = ['fcf_yield', 'roe', 'pb', 'current_ratio', 'debt_equity',
                                'net_margin', 'roa', 'revenue_growth', 'ev_ebitda',
                                'quick_ratio', 'fair_value']
                missing_metrics = [k for k in required_metrics if metrics.get(k) is None]
                if missing_metrics:
                    raise Exception(f"Missing required metrics: {', '.join(missing_metrics)}")
                    
                successful_results[ticker] = metrics
                    
            except Exception as e:
                print(f"Exception fetching {ticker}: {e}")
                if self._check_rate_limit(e):
                    hit_rate_limit = True
                    break
                failed_results[ticker] = {'error': str(e)}
                
        return successful_results, failed_results, hit_rate_limit

    def process_updates(self, batch_size: int = 50):
        candidates = self.get_stocks_to_update()
        if not candidates:
            print("No stocks need updating")
            return

        for i in range(0, len(candidates), batch_size):
            batch = candidates[i:i + batch_size]
            tickers = [stock['ticker'] for stock in batch]
            
            successful_results, failed_results, hit_rate_limit = self.get_metrics_batch(tickers)

            # Process successful updates
            successful_updates = [
                {
                    'rowIndex': stock['rowIndex'],
                    'metrics': successful_results[stock['ticker']]
                }
                for stock in batch
                if stock['ticker'] in successful_results
            ]

            # Process failed updates
            failed_updates = []
            blacklist_updates = []
            for stock in batch:
                if stock['ticker'] in failed_results:
                    error = failed_results[stock['ticker']]['error']
                    update = {
                        'rowIndex': stock['rowIndex'],
                        'failures': stock['failures'],
                        'error': str(error),
                        'ticker': stock['ticker']
                    }
                    if self.should_blacklist(error):
                        blacklist_updates.append(update)
                    failed_updates.append(update)

            # Write all updates for this batch
            if successful_updates:
                self.update_metrics_batch(successful_updates)
            if failed_updates:
                self.update_failures_batch(failed_updates)
            if blacklist_updates:
                self.update_active_status(blacklist_updates)
                self.update_blacklist_sheet(blacklist_updates)
                print(f"Blacklisted {len(blacklist_updates)} stocks")

            if hit_rate_limit:
                print("Rate limit reached - wrote partial results and exiting")
                sys.exit(0)

            time.sleep(0.1)

if __name__ == "__main__":
    SPREADSHEET_ID = os.environ['SPREADSHEET_ID']
    CREDS_PATH = 'creds.json'
    
    updater = FinancialMetricsUpdater(SPREADSHEET_ID, CREDS_PATH)
    updater.process_updates(batch_size=50)