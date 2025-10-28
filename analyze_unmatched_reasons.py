import json
from collections import defaultdict

# Load the unmatched analysis
with open('@debug_transfers_2025_09_30/player_32616_investigation/unmatched_analysis.json', 'r') as f:
    data = json.load(f)

# Categorize by reason
categories = defaultdict(list)

for item in data:
    reason = item['reason']
    pt_id = item['platform_transaction']['id']
    
    # Categorize based on reason content
    if 'No BTs found for account ID 2970' in reason and 'No BT with amount' in reason:
        categories['No FROM amount match + No TO account data'].append(pt_id)
    elif 'date was outside 7-day window' in reason:
        categories['Date outside 7-day window'].append(pt_id)
    elif 'description did not match keywords' in reason and 'No BTs found for account ID 2970' in reason:
        categories['Keyword mismatch on FROM + No TO account data'].append(pt_id)
    elif 'No BT with amount' in reason:
        categories['Amount mismatch'].append(pt_id)
    elif 'description did not match keywords' in reason:
        categories['Keyword mismatch'].append(pt_id)
    elif 'No BTs found for account ID' in reason:
        categories['No bank transactions found'].append(pt_id)
    else:
        categories['Other'].append(pt_id)

# Print summary
print("=" * 80)
print("UNMATCHED TRANSACTIONS BREAKDOWN BY REASON")
print("=" * 80)
print(f"\nTotal Unmatched: {len(data)}\n")

# Sort by count (descending)
sorted_categories = sorted(categories.items(), key=lambda x: len(x[1]), reverse=True)

for category, pt_ids in sorted_categories:
    count = len(pt_ids)
    percentage = (count / len(data)) * 100
    print(f"\n{category}:")
    print(f"  Count: {count} ({percentage:.1f}%)")
    print(f"  Platform IDs: {', '.join(map(str, pt_ids[:5]))}" + 
          (f" ... (+{count-5} more)" if count > 5 else ""))

# Additional analysis: Check TO account issue
print("\n" + "=" * 80)
print("ADDITIONAL INSIGHTS")
print("=" * 80)

to_account_issues = sum(1 for item in data if 'No BTs found for account ID 2970' in item['reason'])
print(f"\nTransactions with TO account (2970) having no bank data: {to_account_issues} ({(to_account_issues/len(data)*100):.1f}%)")

from_keyword_issues = sum(1 for item in data if 'description did not match keywords' in item['reason'])
print(f"Transactions with FROM account keyword mismatch: {from_keyword_issues} ({(from_keyword_issues/len(data)*100):.1f}%)")

# Show some example descriptions that didn't match
print("\n" + "=" * 80)
print("SAMPLE BANK TRANSACTION DESCRIPTIONS THAT DIDN'T MATCH KEYWORDS")
print("=" * 80)

example_descriptions = set()
for item in data:
    reason = item['reason']
    if 'Candidate names:' in reason:
        # Extract the candidate names
        start = reason.index("Candidate names: [") + len("Candidate names: [")
        end = reason.index("]", start)
        names_str = reason[start:end]
        # Parse individual names
        names = [n.strip().strip("'") for n in names_str.split("', '")]
        for name in names[:2]:  # Take first 2 examples
            if name and len(example_descriptions) < 10:
                example_descriptions.add(name)

for i, desc in enumerate(example_descriptions, 1):
    print(f"{i}. {desc}")

print("\n" + "=" * 80)
