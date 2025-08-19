import requests
import json
from datetime import datetime, timedelta
import time 
from zoneinfo import ZoneInfo
import pandas as pd
import os
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from calendar import monthrange

# Load environment variables
load_dotenv()

# Global list to store all orders from all days
all_orders = []

def convert_api_datetime_to_local(date_string):
    """Convert API datetime string from UTC to local time (UTC+3)"""
    if not date_string:
        return None
    utc_time = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("UTC"))
    local_time = utc_time.astimezone(ZoneInfo("Asia/Riyadh"))
    return local_time

def get_month_date_range():
    """Get the date range for the previous month"""
    today = datetime.today()
    # Get first day of current month
    first_day_current = today.replace(day=1)
    # Get last day of previous month
    last_day_previous = first_day_current - timedelta(days=1)
    # Get first day of previous month
    first_day_previous = last_day_previous.replace(day=1)
    
    return first_day_previous, last_day_previous

def operating_single_day(TOKEN, BASE_URL, business_date, order_ref=0):
    """Process orders for a single business date"""
    global all_orders
    
    # Define the endpoint and parameters
    endpoint = "/orders"
    page = 1
    has_more_pages = True
    day_orders = []
    
    print(f"📅 Processing date: {business_date}")
    
    while has_more_pages:
        params = {
            "page": page,
            "filter[business_date]": business_date,
            "filter[status]": "4",
            "include": "branch",
            "sort": "-created_at",
            "filter[reference_after]": order_ref
        }
        
        # Set headers with token
        headers = {
            "Authorization": f"Bearer {TOKEN}"
        }

        try:
            # Make the request
            response = requests.get(BASE_URL + endpoint, headers=headers, params=params)

            # Check response
            if response.status_code == 200:
                data = response.json()
                page_orders = extracting_single_day(data['data'], business_date)
                day_orders.extend(page_orders)

                print(f"    ✅ Page {page}: {len(page_orders)} orders")
                
                meta = data['meta']
                current_page = meta['current_page']
                last_page = meta['last_page']
                
                if current_page >= last_page:
                    has_more_pages = False
                else:
                    page += 1
                    # Small delay between pages
                    time.sleep(0.5)
                
            elif response.status_code == 504:
                print(f"    ❌ Timeout error (504) for {business_date} — skipping this date")
                break
            elif response.status_code == 429:
                print(f"    ⚠️ Rate limited for {business_date} — waiting 10 seconds...")
                time.sleep(10)
                continue
            else:
                print(f"    ❌ Error {response.status_code} for {business_date}: {response.text}")
                break
                
        except requests.exceptions.RequestException as e:
            print(f"    ❌ Request error for {business_date}: {e}")
            break
    
    print(f"    📊 Total orders for {business_date}: {len(day_orders)}")
    all_orders.extend(day_orders)
    
    return len(day_orders)

def extracting_single_day(data, business_date):
    """Extract order data for a single day"""
    day_orders = []
    
    for i in data:
        try:
            branch_id = i['branch']['reference']
            branch_name = i['branch']['name_localized']
            order_ref = i['reference']
            exc_vat_price = i['subtotal_price']
            
            # Fixed: access kitchen times from individual order
            kitchen_rec_str = i.get('meta', {}).get('foodics', {}).get('kitchen_received_at')
            kitchen_done_str = i.get('meta', {}).get('foodics', {}).get('kitchen_done_at')
            
            # Convert to local time
            kitchen_rec = convert_api_datetime_to_local(kitchen_rec_str) if kitchen_rec_str else None
            kitchen_done = convert_api_datetime_to_local(kitchen_done_str) if kitchen_done_str else None
            
            # Calculate period in minutes
            period_minutes = None
            if kitchen_rec and kitchen_done:
                period_minutes = round((kitchen_done - kitchen_rec).total_seconds() / 60, 2)

            # Append to day orders list
            day_orders.append({
                'order_ref': order_ref,
                'branch_id': branch_id,
                'branch_name': branch_name,
                'exc_vat_price': exc_vat_price,
                'business_date': business_date,
                'kitchen_received': kitchen_rec,
                'kitchen_done': kitchen_done,
                'period_minutes': period_minutes
            })
            
        except KeyError as e:
            print(f"        ❌ Missing key in order data: {e}")
            continue
        except Exception as e:
            print(f"        ❌ Error processing order: {e}")
            continue
    
    return day_orders

def operating_monthly(TOKEN, BASE_URL):
    """Process orders for the entire previous month"""
    global all_orders
    all_orders = []  # Reset the list
    
    # Get date range for previous month
    start_date, end_date = get_month_date_range()
    print(f"🗓️ Processing monthly data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Generate list of all dates in the month
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    processed_days = 0
    
    while current_date <= end_date:
        business_date = current_date.strftime("%Y-%m-%d")
        
        # Process single day
        day_count = operating_single_day(TOKEN, BASE_URL, business_date)
        processed_days += 1
        
        print(f"📈 Progress: {processed_days}/{total_days} days processed")
        
        # Move to next day
        current_date += timedelta(days=1)
        
        # Sleep between days to avoid API blocking (skip on last day)
        if current_date <= end_date:
            print(f"    😴 Sleeping 3 seconds before next day...")
            time.sleep(3)
    
    print(f"\n🎉 Monthly processing complete!")
    print(f"📊 Total orders collected: {len(all_orders)}")
    
    # After collecting all data, create DataFrame and Excel
    if all_orders:
        create_monthly_excel_report(start_date, end_date)
    else:
        print("❌ No orders data collected for the month")

def create_monthly_excel_report(start_date, end_date):
    """Create Excel report for monthly data"""
    global all_orders
    
    # Create DataFrame from all collected orders
    df = pd.DataFrame(all_orders)
    
    # Handle timezone issues
    if 'kitchen_received' in df.columns:
        df['kitchen_received'] = df['kitchen_received'].dt.tz_localize(None)
    if 'kitchen_done' in df.columns:
        df['kitchen_done'] = df['kitchen_done'].dt.tz_localize(None)
    
    print(f"📊 Total monthly orders collected: {len(df)}")
    
    # Filter out orders with missing period_minutes
    df_with_periods = df[df['period_minutes'].notna()].copy()
    
    print(f"📊 Monthly orders with valid preparation times: {len(df_with_periods)}")
    
    if len(df_with_periods) == 0:
        print("❌ No orders with valid preparation times found for the month")
        return None
    
    # Create the main monthly report
    branch_report = df_with_periods.groupby(['branch_id', 'branch_name']).agg({
        'period_minutes': ['count', 'mean'],
    }).reset_index()
    
    # Flatten column names
    branch_report.columns = ['branch_code', 'branch_name', 'total_orders', 'average_duration_orders']
    
    # Calculate delayed orders (orders > 15 minutes)
    delayed_orders = df_with_periods[df_with_periods['period_minutes'] > 15].groupby(['branch_id', 'branch_name']).size().reset_index(name='delayed_orders')
    delayed_orders.columns = ['branch_code', 'branch_name', 'delayed_orders']
    
    # Merge the delayed orders data
    branch_report = branch_report.merge(
        delayed_orders[['branch_code', 'delayed_orders']], 
        on='branch_code', 
        how='left'
    )
    
    # Fill NaN values with 0 for branches with no delayed orders
    branch_report['delayed_orders'] = branch_report['delayed_orders'].fillna(0).astype(int)
    
    # Calculate percentage of delayed orders
    branch_report['% of delayed orders'] = (
        (branch_report['delayed_orders'] / branch_report['total_orders']) * 100
    ).round(2)
    
    # Round average duration to 2 decimal places
    branch_report['average_duration_orders'] = branch_report['average_duration_orders'].round(2)
    
    # Reorder columns
    branch_report = branch_report[[
        'branch_code', 
        'branch_name', 
        'total_orders', 
        'delayed_orders', 
        '% of delayed orders', 
        'average_duration_orders'
    ]]
    
    # Daily summary removed as requested
    
    # Create Excel file
    month_year = start_date.strftime("%Y-%m")
    filename = f'/tmp/kitchen_performance_monthly_report_{month_year}.xlsx'
    
    print(f"📁 Saving monthly Excel file to: {filename}")
    
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Main monthly summary by branch only
            branch_report.to_excel(writer, sheet_name='Monthly Branch Summary', index=False)
        
        # Verify file was created successfully
        if os.path.exists(filename):
            file_size = os.path.getsize(filename)
            print(f"✅ Monthly Excel file created successfully: {filename} ({file_size} bytes)")
        else:
            print(f"❌ Failed to create monthly Excel file: {filename}")
            return None
            
    except Exception as e:
        print(f"❌ Error creating monthly Excel file: {e}")
        return None
    
    print(f"📊 Monthly Excel report created: {filename}")
    print(f"\n📈 Monthly Kitchen Performance Report ({start_date.strftime('%B %Y')}):")
    print(branch_report.to_string(index=False))
    
    # Send email with the monthly report
    send_monthly_email_report(filename, start_date, end_date)
    
    return filename

def send_monthly_email_report(filename, start_date, end_date):
    """Send the monthly Excel report via SMTP (Gmail)"""
    try:
        # Email configuration from environment variables
        SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
        SENDER_PASSWORD = os.environ.get('SENDER_PASSWORD')
        RECIPIENT_EMAILS = os.environ.get('RECIPIENT_EMAIL')
        
        if not all([SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_EMAILS]):
            print("❌ Missing email configuration in environment variables")
            print("Required: SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_EMAIL")
            return
        
        email_list = [email.strip() for email in RECIPIENT_EMAILS.split(',')]
        
        # Check if file exists
        if not os.path.exists(filename):
            print(f"❌ File {filename} does not exist!")
            return
        
        # Get file size
        file_size = os.path.getsize(filename)
        print(f"📁 Monthly file size: {file_size} bytes")
        
        # Check file size limit
        if file_size > 25 * 1024 * 1024:  # 25MB
            print(f"❌ Monthly file too large for email: {file_size / 1024 / 1024:.2f}MB")
            return
        
        month_year = start_date.strftime("%B %Y")
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = ', '.join(email_list)
        msg['Subject'] = f'{month_year} - التقرير الشهري لزمن الخدمة'
        
        # Email body
        body = f'''
        <h2>التقرير الشهري لزمن الخدمة</h2>
        <p><strong>{month_year}</strong> مرفق لكم التقرير الشهري لزمن الخدمة لشهر </p>
        <p>فترة التقرير: من {start_date.strftime("%Y-%m-%d")} إلى {end_date.strftime("%Y-%m-%d")}</p>
        <p>الملف يحتوي على:</p>
        <ul>
            <li><strong>ملخص الفروع الشهري:</strong> إجمالي الطلبات والطلبات المتأخرة ومتوسط زمن الخدمة لكل فرع للشهر كاملاً</li>
        </ul>
        <p>تم إنشاء التقرير في: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        '''
        
        msg.attach(MIMEText(body, 'html'))
        
        # Attach Excel file
        try:
            with open(filename, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            
            attachment_filename = os.path.basename(filename)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {attachment_filename}'
            )
            
            msg.attach(part)
            print(f"✅ Monthly attachment added: {attachment_filename}")
            
        except Exception as attach_error:
            print(f"❌ Error creating monthly attachment: {attach_error}")
            return
        
        # Send email via Gmail SMTP
        print(f"📧 Sending monthly report email...")
        
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        
        text = msg.as_string()
        server.sendmail(SENDER_EMAIL, email_list, text)
        server.quit()
        
        print("✅ Monthly email sent successfully!")
        
        # Clean up: Delete the temporary file after sending
        try:
            os.remove(filename)
            print(f"🗑️ Monthly temporary file deleted: {filename}")
        except Exception as cleanup_error:
            print(f"⚠️ Could not delete monthly temporary file: {cleanup_error}")
        
    except smtplib.SMTPAuthenticationError:
        print("❌ SMTP Authentication failed!")
        print("Make sure you're using a Gmail App Password")
    except smtplib.SMTPException as smtp_error:
        print(f"❌ SMTP Error: {smtp_error}")
    except Exception as e:
        print(f"❌ Error sending monthly email: {e}")
        import traceback
        traceback.print_exc()

# Main execution
if __name__ == "__main__":
    TOKEN = os.environ.get('API_TOKEN')
    BASE_URL = os.environ.get('BASE_URL')
    
    if not TOKEN or not BASE_URL:
        print("❌ Missing API_TOKEN or BASE_URL in environment variables")
    else:
        print("🗓️ Starting Monthly Kitchen Performance Report Generation...")
        operating_monthly(TOKEN, BASE_URL)