import os
import csv
import time
import sys
import inspect
from datetime import datetime, timezone, timedelta
from functools import wraps
from atproto import Client, exceptions
from dotenv import load_dotenv
from enum import Enum
from tqdm import tqdm

# Load environment variables
load_dotenv(override=True)

date_format = "%Y-%m-%d %H:%M:%S.%f%z"

# Get credentials from .env
username = os.getenv("BLUESKY_USERNAME")
password = os.getenv("BLUESKY_PASSWORD")

# Initialize the client
client = Client()
my_followers = set()
accounts_i_follow = set()
my_followers_filename = "my_followers.csv"  # CSV file containing followers
accounts_i_follow_filename = "accounts_i_follow.csv"  # CSV file containing accounts I follow
followback_account_filename = "followback_accounts.csv"  # CSV file containing handles
accounts_to_follow_filename = "accounts_to_follow.csv"  # CSV file containing accounts to follow
removed_users_filename = "removed_users.csv"  # CSV file containing removed users
do_not_remove_filename = "do_not_remove.csv"  # CSV file containing users not to remove
added_by_API_filename = "added_by_API.csv"  # CSV file containing users added by API
manual_removal_filename = "manual_remove.csv"  # CSV file containing users to remove manually
followed_manually_filename = "followed_manually.csv"  # CSV file containing users followed manually


class RelationshipType(Enum):
    FOLLOWERS = "followers"
    FOLLOWS = "follows"


class ModifyFollowers(Enum):
    FOLLOW = "follow"
    UNFOLLOW = "unfollow"


def handle_error(exception: Exception):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    current_frame = inspect.currentframe()
    caller_frame = current_frame.f_back if current_frame else None
    function_name = caller_frame.f_code.co_name if caller_frame else 'Unknown'
    
    # Get the file name and line number where the exception occurred
    file_name = exc_traceback.tb_frame.f_code.co_filename
    line_number = exc_traceback.tb_lineno
    
    print(
        f"Error in function '{function_name}' - {exception}. "
        f"Exception Raised In: {exc_traceback.tb_frame.f_code.co_name} - "
        f"File: {file_name} - Line: {line_number} - "
        f"Type: {exc_type}. "
    )
    return

def rate_limiter(interval):
    """
    A decorator to limit the execution of a function to once every `interval` seconds,
    with built-in wait functionality.
    """
    def decorator(func):
        last_called = [0]  # Use a mutable object to store state

        @wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            elapsed = now - last_called[0]
            if elapsed < interval:
                time.sleep(interval - elapsed)  # Wait until the next allowed call
            last_called[0] = time.time()
            return func(*args, **kwargs)
        return wrapper
    return decorator


def init():
    global my_followers, accounts_i_follow
    try:
        followers = fetch_relationships(client, username, RelationshipType.FOLLOWERS)
        my_followers = set([account.did for account in followers])
        followers_dict = [{'Handle': account.handle, 'Display Name': account.display_name, 'DID': account.did, 'Follows Me': True} for account in followers]
        save_accounts_to_csv(my_followers_filename, followers_dict)

        follows = fetch_relationships(client, username, RelationshipType.FOLLOWS)
        accounts_i_follow = set([account.did for account in follows])
        follows_dict = [{'Handle': account.handle, 'Display Name': account.display_name, 'DID': account.did, 'Follows Me': is_following_me(client, account.did)} for account in follows]
        save_accounts_to_csv(accounts_i_follow_filename, follows_dict)

        added_by_API = read_accounts_from_csv(added_by_API_filename)
        added_manually = my_followers - set([account['DID'] for account in added_by_API])
        added_manually_dict = [account for account in followers_dict if account['DID'] in added_manually]
        save_accounts_to_csv(followed_manually_filename, added_manually_dict)
    except Exception as e:
        print(f"Failed to fetch followers for {username}: {e}")
        raise e
    return


def get_relationships_of_handle_list(handles=[], relationship_type: RelationshipType = RelationshipType.FOLLOWERS, testing=False):
    try:
        # Fetch accounts following a sample handle
        data = []
        for handle in handles:
            following = fetch_relationships(client, handle, relationship_type, testing = testing)
            # Prepare data for CSV
            for account in following:
                handle = account.handle
                display_name = account.display_name
                did = account.did
                follows_me = True if did in my_followers else False
                data.append([handle, display_name, did, follows_me])
        return_dict = [
            {
                'Handle': datum[0],
                'Display Name': datum[1],
                'DID': datum[2],
                'Follows Me': datum[3]
            }
            for datum in data 
        ]

    except Exception as e:
        print(f"An error occurred: {e}")
    return return_dict


def fetch_relationships(client, handle, relationship_type, testing=False):
    """
    Fetch all accounts that a given handle is associated with.
    """
    print(f"Fetching {relationship_type.value} for {handle}...")
    accounts = []
    cursor = None
    i=0
    while True:
        try:
            if relationship_type == RelationshipType.FOLLOWERS:
                response = client.get_followers(handle, cursor=cursor)
                accounts.extend(response.followers)
            elif relationship_type == RelationshipType.FOLLOWS:
                response = client.get_follows(handle, cursor=cursor)
                accounts.extend(response.follows)
            print(f"Fetched {len(accounts)} {relationship_type.value} for {handle} so far...")
            i+=1

            # Get the next cursor
            cursor = response.cursor
            if not cursor:  # If no cursor, exit the loop
                break

            if testing and i >= 2:
                break   # Limit the number of accounts fetched in testing mode

        except exceptions.InvokeTimeoutError as e:
            print(f"Timeout error: {e}")
            continue

        except Exception as e:
            print(f"Failed to fetch {relationship_type.value} for {handle}: {e}")
            raise e

    print(f"Found {len(accounts)} {relationship_type.value} for {handle}")
    return accounts


def is_following_me(client, account_did):
    """
    Check if an account is following the logged-in user.
    """
    global my_followers
    try:
        #if account_did is member of my_followers set, return True
        if account_did in my_followers:
            return True
        return False
    except Exception as e:
        print(f"Failed to check if {account_did} is following: {e}")
        return False


def unfollow_accounts_not_following_me():
    """
    Unfollow accounts that I am following but are not following me back.
    """
    global accounts_i_follow, my_followers
    try:
        dids_added_by_API = set(account['DID'] for account in read_accounts_from_csv(added_by_API_filename))
        do_not_remove_handles = set(handle for handle in read_handles_from_csv(do_not_remove_filename))
        accounts_i_follow_added_by_API = accounts_i_follow & dids_added_by_API

        # Find accounts to unfollow, starting with only those that were added to my followership by this script
        dids_to_unfollow = accounts_i_follow_added_by_API - my_followers
        account_data_of_follows = read_accounts_from_csv(accounts_i_follow_filename)
        accounts_to_unfollow = [account for account in account_data_of_follows if account['DID'] in dids_to_unfollow and account['Handle'] not in do_not_remove_handles]
        print(f"Unfollowing {len(accounts_to_unfollow)} accounts...")
        save_accounts_to_csv(removed_users_filename, accounts_to_unfollow)
        #for account in accounts_to_unfollow:
        #    unfollow_account(account)
    except Exception as e:
        print(f"Failed to unfollow accounts: {e}")
    return


def modify_followers(data, modification=ModifyFollowers.FOLLOW, testing=False):
    """
    Follow accounts that are following me.
    """
    global my_followers, accounts_i_follow

    try:
        if isinstance(data, list):
            accounts_to_modify = data
        elif isinstance(data, str):
            accounts_to_modify= read_accounts_from_csv(data)
        else:
            raise ValueError("Data must be a dictionary or a filename.")
        
        # If testing is True, limit the number of accounts to follow to 20
        if testing:
            accounts_to_modify = accounts_to_modify[:200]
        
        print(f"Modifying {len(accounts_to_modify)} accounts...")   
        newly_modified_accounts = []
        for account in accounts_to_modify:
            success = follow_or_unfollow(account, modification)
            if success:
                newly_modified_accounts.append(account)

        #if modification == ModifyFollowers.FOLLOW:   
        #    # Write the followed accounts to the added_by_API_filename CSV
        #    add_new_accounts_to_csv(added_by_API_filename, [[account['Handle'], account['Display Name'], account['DID'], True, datetime.now(timezone.utc)] for account in newly_modified_accounts])
        #    # Update the accounts_i_follow_filename CSV with the new followed accounts
        #    add_new_accounts_to_csv(accounts_i_follow_filename, [[account['Handle'], account['Display Name'], account['DID'], True] for account in newly_modified_accounts])
        #elif modification == ModifyFollowers.UNFOLLOW:
        #    # Remove the unfollowed accounts from the accounts_i_follow_filename CSV
        #    remove_accounts_from_csv(accounts_i_follow_filename, [[account['Handle'], account['Display Name'], account['DID'], True] for account in newly_modified_accounts])
        #    # Add the unfollowed accounts to the removed_users_filename CSV
        #    add_new_accounts_to_csv(removed_users_filename, [[account['Handle'], account['Display Name'], account['DID'], True, datetime.now(timezone.utc)] for account in newly_modified_accounts])
    except Exception as e:
        handle_error(e)
    return newly_modified_accounts


@rate_limiter(3)
def follow_or_unfollow(account, modification=ModifyFollowers.FOLLOW):
    global accounts_i_follow
    try:
        if modification == ModifyFollowers.FOLLOW:
            client.follow(account['DID'])
            accounts_i_follow.add(account['DID'])
            print(f"Followed account {account['Handle']} ({account['DID']})")

            # Write the followed account to the added_by_API_filename CSV
            add_new_accounts_to_csv(added_by_API_filename, [[account['Handle'], account['Display Name'], account['DID'], True, datetime.now(timezone.utc)]])
            # Update the accounts_i_follow_filename CSV with the new followed account
            add_new_accounts_to_csv(accounts_i_follow_filename, [[account['Handle'], account['Display Name'], account['DID'], True]])

        elif modification == ModifyFollowers.UNFOLLOW:
            account_to_unfollow = client.follow(account['DID'])
            URI = account_to_unfollow.uri
            client.unfollow(URI)
            accounts_i_follow.remove(account['DID'])
            print(f"Unfollowed account {account['Handle']} ({account['DID']})")

            # Remove the unfollowed account from the accounts_i_follow_filename CSV
            remove_accounts_from_csv(accounts_i_follow_filename, [[account['Handle'], account['Display Name'], account['DID'], True]])
            # Add the unfollowed account to the removed_users_filename CSV
            add_new_accounts_to_csv(removed_users_filename, [[account['Handle'], account['Display Name'], account['DID'], True, datetime.now(timezone.utc)]])
        return True
    except Exception as e:
        print(f"Failed to follow account {account['Handle']} ({account['DID']}): {e}")
        return False


#########################
# CSV Utility Functions #
#########################



def save_accounts_to_csv(filename, accounts):
    # Fetch accounts following a sample handle
    data = []
    # Prepare data for CSV
    for account in accounts:
        handle = account['Handle']
        display_name = account['Display Name']
        did = account['DID']
        follows_me = account['Follows Me']
        data.append([handle, display_name, did, follows_me])
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(["Handle", "Display Name", "DID", "Follows Me"])
        # Write data
        writer.writerows(data)
    print(f"Data saved to {filename}")
    return


def read_accounts_from_csv(filename):
    """
    Read data from a CSV file and return it as a dictionary.
    """
    data = []
    try:
        with open(filename, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)
    except FileNotFoundError:
        # If the file does not exist, return an empty dictionary
        pass
    except Exception as e:
        print(f"Failed to read data from {filename}: {e}")
    return data


def add_new_accounts_to_csv(filename, data):
    """
    Add new data points to a CSV file if they are not already present.
    """
    existing_data = read_handles_from_csv(filename)

    new_data = []
    for row in data:
        handle = row[0]  # Assuming the first column is "Handle"
        if handle not in existing_data:
            new_data.append(row)

    if new_data:
        with open(filename, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerows(new_data)
        print(f"Added {len(new_data)} new rows to {filename}")
    else:
        print(f"No new data to add to {filename}")
    return


def remove_accounts_from_csv(filename, data):
    """
    Remove data points from a CSV file.
    """
    existing_data = read_accounts_from_csv(filename)

    # Create a set of handles to remove for faster lookup
    handles_to_remove = {row[0] for row in data}  # Assuming the first column is "Handle"

    # Filter out the rows that should be removed
    remaining_data = [row for row in existing_data if row['Handle'] not in handles_to_remove]

    # Write the remaining data back to the CSV file
    if remaining_data:
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=existing_data[0].keys())
            writer.writeheader()
            writer.writerows(remaining_data)
        print(f"Removed {len(existing_data) - len(remaining_data)} rows from {filename}")
    else:
        print(f"No data to remove from {filename}")
    return


def read_handles_from_csv(filename):
    """
    Read handles from a CSV file and return them as a list.
    """
    handles = []
    try:
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                handles.append(row['Handle'])
    except Exception as e:
        print(f"Failed to read handles from {filename}: {e}")
    return handles


def main():
    # sample_handle = "donkoclock.bsky.social"  # Replace with the target handle
    max_follows = 2000
    testing = False
    keep_for_x_days = 1 if datetime.now().weekday() != 5 else 0.0001  # On all days except Saturday, unfollow accounts added by this script after 1 day. On Saturday unfollow everyone.
    followback_accounts = read_handles_from_csv(followback_account_filename)  # Read handles from CSV

    def construct_follow_list(prospects):
        already_added = set([row['DID'] for row in read_accounts_from_csv(added_by_API_filename)])
        existing_relationships = accounts_i_follow.union(my_followers)
        do_not_follow = existing_relationships.union(already_added)
        accounts_to_follow=[]
        i=0

        # Initialize the progress bar
        with tqdm(total=max_follows, desc="Constructing follow list", unit="account") as pbar:
            for account in prospects:
                if account['DID'] not in do_not_follow:
                    accounts_to_follow.append(account)
                    i+=1
                if i >= max_follows:
                    break
                pbar.update(1)
        return accounts_to_follow
    
    try:
        # Authenticate
        client.login(username, password)
        print("Authenticated successfully!")
        init()

        # Step 1: Unfollow all accounts added by this script over X days ago or on the manual-remove list.
        print(f"Step 1: Unfollowing accounts added by this script over {keep_for_x_days} days ago, or that are on the remove list...")
        time.sleep(0.5)
        account_dict = read_accounts_from_csv(added_by_API_filename)
        do_not_remove_list = read_handles_from_csv(do_not_remove_filename)
        accounts_to_unfollow = [
            account for account in account_dict
            if account['Add Date'] is not None and
            datetime.strptime(account['Add Date'], date_format) < datetime.now(timezone.utc) - timedelta(days=keep_for_x_days) and
            account['DID'] in accounts_i_follow and
            account['Handle'] not in do_not_remove_list
        ]
        manual_remove_list = read_accounts_from_csv(manual_removal_filename)
        accounts_to_unfollow.extend([
            account for account in manual_remove_list
            if account['DID'] in accounts_i_follow and
            account['Handle'] not in do_not_remove_list
        ])

        # Step 2: Follow some of the followers of the handles in the "followback_accounts.csv" CSV file
        
        # Check if today is Saturday
        if datetime.now().weekday() != 5:  # 5 corresponds to Saturday
            print("Step 2: Getting the followers of the followback accounts...")
            time.sleep(0.5)
            prospects = get_relationships_of_handle_list(handles=followback_accounts, relationship_type=RelationshipType.FOLLOWERS, testing=testing)
            new_accounts_to_follow = construct_follow_list(prospects)
            modify_followers(new_accounts_to_follow, ModifyFollowers.FOLLOW, testing=testing)
            
        
        """
        # Step 3: Follow the account that are following me.
        print("Step 3: Following accounts that are following me...")
        time.sleep(0.5)
        accounts_to_follow = [account for account in read_accounts_from_csv(my_followers_filename) 
                              if account['DID'] not in accounts_i_follow and
                                account['DID'] not in added_by_API_dids and
                                account['DID'] not in removed_user_dids and
                                account['DID'] not in manual_removal_dids
                              ]
        modify_followers(accounts_to_follow, ModifyFollowers.FOLLOW, testing=testing)
        """

        """
        # Step 4: Unfollow accounts I followed using this app, but that are following me back after a certain amoung to time
        print("Step 4: Unfollowing accounts that are not following me back whin 2 days...")
        time.sleep(0.5)
        account_dict = read_accounts_from_csv(added_by_API_filename)
        accounts_that_didnt_follow_back = [
            account for account in account_dict
            if account['DID'] not in my_followers and
            account['Add Date'] is not None and
            datetime.strptime(account['Add Date'], date_format) < datetime.now(timezone.utc) - timedelta(days=2)
        ]
        modify_followers(accounts_that_didnt_follow_back, ModifyFollowers.UNFOLLOW, testing=testing)
        """


        modify_followers(accounts_to_unfollow, ModifyFollowers.UNFOLLOW, testing=testing)

        print("All steps completed successfully!")

    except Exception as e:
        handle_error(e)


#If run as main.py
if __name__ == "__main__":
    main()