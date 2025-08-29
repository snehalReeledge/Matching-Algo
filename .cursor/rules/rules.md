# Received/Returned Transaction Matching Algorithm

A modular, optimized Python system for matching returned platform transactions with bank transactions based on amount, date, and keywords.

## 🏗️ Modular Architecture

The system has been broken down into separate, focused modules for better maintainability and reusability:

### Core Modules

- **`config.py`** - Configuration constants and settings
- **`models.py`** - Data structures and dataclasses
- **`api_client.py`** - HTTP client with caching and connection pooling
- **`returned_transaction_matcher.py`** - Core matching logic
- **`result_formatter.py`** - Result formatting and display
- **`orchestrator.py`** - Process coordination and parallel processing
- **`api_tester.py`** - API endpoint testing utilities
- **`main.py`** - Main entry point and demonstration

## 🚀 Features

- **Parallel Processing**: Uses ThreadPoolExecutor for concurrent API calls
- **Batch Operations**: Processes players in batches to reduce overhead
- **Connection Pooling**: Optimized HTTP connections with retry strategies
- **In-Memory Caching**: TTL-based caching for API responses
- **Optimized Algorithms**: Pre-processed data structures for faster matching
- **Error Handling**: Comprehensive error handling and logging
- **Modular Design**: Easy to maintain, test, and extend

## 📋 Requirements

- Python 3.7+
- `requests` library
- `urllib3` library

Install dependencies:
```bash
pip install -r requirements.txt
```

## 🔧 Configuration

Edit `config.py` to customize:
- API endpoints
- Performance settings (workers, batch size, timeouts)
- Returned transaction keywords
- Logging levels

## 📖 Usage

### Basic Usage

```python
from orchestrator import FastTransactionMatchingOrchestrator

# Create orchestrator
orchestrator = FastTransactionMatchingOrchestrator()

# Process all players
results = orchestrator.process_all_players()

# Process single player
player_result = orchestrator.process_single_player(player_id=24)
```

### API Testing

```python
from api_tester import test_api_endpoints, test_single_player_matching

# Test all endpoints
test_api_endpoints()

# Test specific player
test_single_player_matching(player_id=24)
```

### Custom Matching

```python
from returned_transaction_matcher import OptimizedTransactionMatcher
from models import MatchResults

# Create matcher with custom data
matcher = OptimizedTransactionMatcher(
    platform_transactions=your_platform_data,
    bank_transactions=your_bank_data,
    returned_keywords=["returned", "refund"]
)

# Perform matching
results = matcher.match_returned_transactions()
```

## 🏃‍♂️ Running the System

### Run Main Program
```bash
python main.py
```

### Test API Endpoints
```bash
python api_tester.py
```

### Import as Module
```python
from main import main, process_single_player_demo

# Run full system
main()

# Test single player
process_single_player_demo(player_id=24)
```

## 📊 Output Format

The system returns structured data including:

- **Matches**: Successfully matched transaction pairs
- **Summary Statistics**: Match rates, counts, and performance metrics
- **Unmatched Transactions**: Transactions that couldn't be matched
- **Performance Metrics**: Processing times and efficiency data

## 🔍 API Endpoints

- **Players**: `https://xhks-nxia-vlqr.n7c.xano.io/api:G9pW0Uty/players`
- **Bank Transactions**: `https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/getUserBankTransactions`
- **Platform Transactions**: `https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/getTransactions`

## 🧪 Testing

The system includes built-in testing capabilities:

1. **API Endpoint Testing**: Verifies all endpoints are accessible
2. **Single Player Testing**: Tests matching logic on individual players
3. **Performance Testing**: Measures processing times and efficiency

## 📈 Performance Optimizations

- **Parallel Processing**: Multiple workers for concurrent API calls
- **Batch Operations**: Reduced network overhead through batching
- **Connection Pooling**: Reuses HTTP connections
- **Caching**: TTL-based caching for API responses
- **Pre-processing**: Optimized data structures for faster matching

## 🛠️ Customization

### Adding New Matching Criteria

```python
# In returned_transaction_matcher.py
def _custom_matching_logic(self, platform_transaction, bank_transaction):
    # Add your custom logic here
    return custom_match_result
```

### Modifying API Endpoints

```python
# In config.py
CUSTOM_API_URL = "https://your-api-endpoint.com/api"
```

### Adding New Result Formats

```python
# In result_formatter.py
@staticmethod
def format_custom_results(results):
    # Add your custom formatting logic
    return formatted_results
```

## 🐛 Troubleshooting

### Common Issues

1. **API Connection Errors**: Check endpoint URLs and network connectivity
2. **Performance Issues**: Adjust `MAX_WORKERS` and `BATCH_SIZE` in config
3. **Memory Issues**: Reduce batch size or implement pagination
4. **Authentication Errors**: Verify API credentials and permissions

### Debug Mode

Enable debug logging by changing `LOG_LEVEL` in `config.py`:
```python
LOG_LEVEL = "DEBUG"  # For detailed logging
```

## 📝 License

This project is proprietary software. All rights reserved.

## 🤝 Support

For support and questions, contact the development team or refer to the API documentation.
