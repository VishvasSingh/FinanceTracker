import gspread
import pandas as pd
import datetime
import re
from bs4 import BeautifulSoup
from gspread_dataframe import set_with_dataframe
from typing import Tuple


current_date = datetime.datetime.utcnow()
current_month_abbr = current_date.strftime('%b')
current_year = current_date.year


def is_current_month_transaction(transaction_text: str) -> bool:
    match = re.search(r'([A-Za-z]{3})\s+\d{1,2},\s+(\d{4})', transaction_text)
    if match:
        month_abbr = match.group(1)
        year = int(match.group(2))
        is_current_month = (month_abbr == current_month_abbr and year == current_year)
        return is_current_month

    return False


def extract_transaction_details(transaction_text: str) -> Tuple[str, str, str]:
    # Extract amount
    amount_match = re.search(r'₹([\d,.]+)', transaction_text)
    amount = amount_match.group(1) if amount_match else 'Unavailable'

    # Extract recipient
    recipient_match = re.search(r'to (.*?) using', transaction_text)
    recipient = recipient_match.group(1).strip() if recipient_match else 'Unavailable'

    # Extract date and time
    datetime_match = re.search(r'([A-Za-z]{3} \d{1,2}, \d{4}, [\d:]+ [AP]M)', transaction_text)
    transaction_time = datetime_match.group(1) if datetime_match else 'Unavailable'

    return amount, recipient, transaction_time


def parse_html_transactions_file(html_content: bytes) -> pd.DataFrame:
    """
        Parses the HTML content from a Google Pay transaction report to extract transactions.

        Args:
            html_content (bytes): The byte content of the HTML file.

        Returns:
            pd.DataFrame: A DataFrame containing the parsed transaction data with columns
                          ['Date', 'Merchant', 'Amount', 'Status']. Returns an empty DataFrame
                          if no transactions are found.
    """

    soup = BeautifulSoup(html_content, 'html.parser')
    transactions = []
    transaction_blocks = soup.find_all('div',
                                       class_='content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1')

    current_month_transactions = []
    for each_transaction_block in transaction_blocks:
        # filtering out only current month transactions
        if is_current_month_transaction(each_transaction_block.text):
            current_month_transactions.append(each_transaction_block)
        else:
            # the transactions are in sorted order, if we have encountered a previous month transaction then all the
            # next transactions are going to be of previous months
            break

    for each_transaction_block in current_month_transactions:
        amount, recipient, transaction_time = extract_transaction_details(each_transaction_block.text)
        transactions.append(
            {
                'Date': transaction_time,
                'Description': 'TBD',
                'Amount': amount,
                'Paid to': recipient
            }
        )

    if not transactions:
        return pd.DataFrame()

    return pd.DataFrame(transactions)



def write_to_spreadsheet(df: pd.DataFrame):
    gc = gspread.service_account(filename='finance-tracker-472017-6f46eb4aff9b.json')
    sh = gc.open('test finance tracker').sheet1
    set_with_dataframe(sh,df)
    print("Finance sheet updated successfully!")


if __name__ == "__main__":
    with open('My Activity.html', 'rb') as f:
        transactions_html_content = f.read()

    df = parse_html_transactions_file(html_content=transactions_html_content)
    write_to_spreadsheet(df)