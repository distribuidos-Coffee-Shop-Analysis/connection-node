import unittest
from protocol.messages import (
    DatasetType,
    MenuItemRecord,
    StoreRecord,
    TransactionItemRecord,
    TransactionRecord,
    UserRecord,
    Q1Record,
    Q2Record,
    Q3Record,
    Q4Record,
    BatchMessage,
    MESSAGE_TYPE_BATCH,
    _create_record_from_string,
)
from common.utils import get_joiner_partition


class TestCoffeeShopRecords(unittest.TestCase):

    def test_menu_item_record_serialization(self):
        """Test MenuItemRecord serialization and deserialization"""
        record = MenuItemRecord(
            "1", "Espresso", "Coffee", "3.50", "false", "2023-01-01", "2023-12-31"
        )

        # Test serialization
        serialized = record.serialize()
        expected = "1|Espresso|Coffee|3.50|false|2023-01-01|2023-12-31"
        self.assertEqual(serialized, expected)

        # Test deserialization
        deserialized = MenuItemRecord.from_string(serialized)
        self.assertEqual(deserialized.item_id, "1")
        self.assertEqual(deserialized.item_name, "Espresso")
        self.assertEqual(deserialized.category, "Coffee")
        self.assertEqual(deserialized.price, "3.50")

        # Test dataset type
        self.assertEqual(record.get_type(), DatasetType.MENU_ITEMS)

    def test_store_record_serialization(self):
        """Test StoreRecord serialization and deserialization"""
        record = StoreRecord(
            "101",
            "Downtown Cafe",
            "123 Main St",
            "12345",
            "NYC",
            "NY",
            "40.7128",
            "-74.0060",
        )

        # Test serialization
        serialized = record.serialize()
        expected = "101|Downtown Cafe|123 Main St|12345|NYC|NY|40.7128|-74.0060"
        self.assertEqual(serialized, expected)

        # Test deserialization
        deserialized = StoreRecord.from_string(serialized)
        self.assertEqual(deserialized.store_id, "101")
        self.assertEqual(deserialized.store_name, "Downtown Cafe")
        self.assertEqual(deserialized.city, "NYC")

        # Test dataset type
        self.assertEqual(record.get_type(), DatasetType.STORES)

    def test_transaction_record_serialization(self):
        """Test TransactionRecord serialization and deserialization"""
        record = TransactionRecord(
            "T001", "101", "CASH", "V001", "U001", "10.00", "1.00", "9.00", "2023-01-15"
        )

        # Test serialization
        serialized = record.serialize()
        expected = "T001|101|CASH|V001|U001|10.00|1.00|9.00|2023-01-15"
        self.assertEqual(serialized, expected)

        # Test deserialization
        deserialized = TransactionRecord.from_string(serialized)
        self.assertEqual(deserialized.transaction_id, "T001")
        self.assertEqual(deserialized.final_amount, "9.00")

        # Test dataset type
        self.assertEqual(record.get_type(), DatasetType.TRANSACTIONS)

    def test_query_response_records(self):
        """Test query response record types"""
        # Test Q1Record
        q1_record = Q1Record("T001", "25.50")
        self.assertEqual(q1_record.serialize(), "T001|25.50")
        self.assertEqual(q1_record.get_type(), DatasetType.Q1)

        # Test Q2Record
        q2_record = Q2Record("2023-01", "Espresso", "150")
        self.assertEqual(q2_record.serialize(), "2023-01|Espresso|150")
        self.assertEqual(q2_record.get_type(), DatasetType.Q2)

        # Test Q3Record
        q3_record = Q3Record("2023-H1", "Downtown Cafe", "1250.75")
        self.assertEqual(q3_record.serialize(), "2023-H1|Downtown Cafe|1250.75")
        self.assertEqual(q3_record.get_type(), DatasetType.Q3)

        # Test Q4Record
        q4_record = Q4Record("Downtown Cafe", "1990-05-15")
        self.assertEqual(q4_record.serialize(), "Downtown Cafe|1990-05-15")
        self.assertEqual(q4_record.get_type(), DatasetType.Q4)

    def test_batch_message_creation(self):
        """Test BatchMessage creation and basic properties"""
        records = [
            MenuItemRecord(
                "1", "Espresso", "Coffee", "3.50", "false", "2023-01-01", "2023-12-31"
            ),
            MenuItemRecord(
                "2", "Latte", "Coffee", "4.50", "false", "2023-01-01", "2023-12-31"
            ),
        ]

        batch = BatchMessage(DatasetType.MENU_ITEMS, records, eof=True)

        self.assertEqual(batch.type, MESSAGE_TYPE_BATCH)
        self.assertEqual(batch.dataset_type, DatasetType.MENU_ITEMS)
        self.assertEqual(len(batch.records), 2)
        self.assertTrue(batch.eof)

    def test_create_record_from_string_factory(self):
        """Test the factory function for creating records from strings"""
        # Test MenuItemRecord creation
        menu_data = "1|Espresso|Coffee|3.50|false|2023-01-01|2023-12-31"
        menu_record = _create_record_from_string(DatasetType.MENU_ITEMS, menu_data)
        self.assertIsInstance(menu_record, MenuItemRecord)
        self.assertEqual(menu_record.item_name, "Espresso")

        # Test StoreRecord creation
        store_data = "101|Downtown Cafe|123 Main St|12345|NYC|NY|40.7128|-74.0060"
        store_record = _create_record_from_string(DatasetType.STORES, store_data)
        self.assertIsInstance(store_record, StoreRecord)
        self.assertEqual(store_record.store_name, "Downtown Cafe")

        # Test Q1Record creation
        q1_data = "T001|25.50"
        q1_record = _create_record_from_string(DatasetType.Q1, q1_data)
        self.assertIsInstance(q1_record, Q1Record)
        self.assertEqual(q1_record.final_amount, "25.50")

    def test_invalid_dataset_type_raises_error(self):
        """Test that invalid dataset type raises ValueError"""
        with self.assertRaises(ValueError):
            _create_record_from_string(999, "invalid,data")


class TestDatasetTypes(unittest.TestCase):

    def test_dataset_type_constants(self):
        """Test that dataset type constants have correct values"""
        # Input datasets
        self.assertEqual(DatasetType.MENU_ITEMS, 1)
        self.assertEqual(DatasetType.STORES, 2)
        self.assertEqual(DatasetType.TRANSACTION_ITEMS, 3)
        self.assertEqual(DatasetType.TRANSACTIONS, 4)
        self.assertEqual(DatasetType.USERS, 5)

        # Query responses
        self.assertEqual(DatasetType.Q1, 10)
        self.assertEqual(DatasetType.Q2, 11)
        self.assertEqual(DatasetType.Q3, 12)
        self.assertEqual(DatasetType.Q4, 13)


class TestJoinerPartitioning(unittest.TestCase):

    def test_get_joiner_partition_deterministic(self):
        """Test that the same user_id always maps to the same partition"""
        user_id = "12345"
        users_joiners_count = 3
        
        # Call multiple times with same user_id
        partition1 = get_joiner_partition(user_id, users_joiners_count)
        partition2 = get_joiner_partition(user_id, users_joiners_count)
        partition3 = get_joiner_partition(user_id, users_joiners_count)
        
        # Should always return the same partition
        self.assertEqual(partition1, partition2)
        self.assertEqual(partition2, partition3)

    def test_get_joiner_partition_range(self):
        """Test that partitions are within valid range"""
        users_joiners_count = 5
        
        # Test with multiple user IDs
        for user_id in ["1", "100", "999", "12345", "67890"]:
            partition = get_joiner_partition(user_id, users_joiners_count)
            
            # Partition should be between 0 and users_joiners_count-1
            self.assertGreaterEqual(partition, 0)
            self.assertLess(partition, users_joiners_count)

    def test_get_joiner_partition_distribution(self):
        """Test that different user_ids get distributed across partitions"""
        users_joiners_count = 3
        partitions_used = set()
        
        # Generate partitions for multiple users
        for i in range(100):
            user_id = str(i)
            partition = get_joiner_partition(user_id, users_joiners_count)
            partitions_used.add(partition)
        
        # With 100 users and 3 partitions, we should use all partitions
        # (extremely unlikely not to with good hash distribution)
        self.assertEqual(len(partitions_used), users_joiners_count)

    def test_get_joiner_partition_single_joiner(self):
        """Test that with a single joiner, all users go to partition 0"""
        users_joiners_count = 1
        
        for user_id in ["1", "100", "999", "12345"]:
            partition = get_joiner_partition(user_id, users_joiners_count)
            self.assertEqual(partition, 0)

    def test_get_joiner_partition_string_and_int_consistency(self):
        """Test that user_id as string or int produces same partition"""
        users_joiners_count = 5
        user_id_int = 12345
        user_id_str = "12345"
        
        partition_int = get_joiner_partition(user_id_int, users_joiners_count)
        partition_str = get_joiner_partition(user_id_str, users_joiners_count)
        
        # Both should map to same partition since we convert to string internally
        self.assertEqual(partition_int, partition_str)


if __name__ == "__main__":
    unittest.main()
