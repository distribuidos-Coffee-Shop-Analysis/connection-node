# Message types (matching Go client)
MESSAGE_TYPE_BATCH = 1
MESSAGE_TYPE_RESPONSE = 2

import logging


class DatasetType:
    MENU_ITEMS = 1
    STORES = 2
    TRANSACTION_ITEMS = 3
    TRANSACTIONS = 4
    USERS = 5
    CLEANUP = 7

    Q1 = 6
    Q2 = 9
    Q3 = 12
    Q4 = 16


class Record:
    """Base interface for all record types"""

    def serialize(self):
        """Serialize record to string format"""
        raise NotImplementedError

    def get_type(self):
        """Get the dataset type of this record"""
        raise NotImplementedError


# Input dataset records
class MenuItemRecord(Record):
    """Menu item record: item_id, item_name, category, price, is_seasonal, available_from, available_to"""

    PARTS = 7

    def __init__(
        self,
        item_id,
        item_name,
        category,
        price,
        is_seasonal,
        available_from,
        available_to,
    ):
        self.item_id = item_id
        self.item_name = item_name
        self.category = category
        self.price = price
        self.is_seasonal = is_seasonal
        self.available_from = available_from
        self.available_to = available_to

    def serialize(self):
        return f"{self.item_id}|{self.item_name}|{self.category}|{self.price}|{self.is_seasonal}|{self.available_from}|{self.available_to}"

    def get_type(self):
        return DatasetType.MENU_ITEMS

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid MenuItemRecord format: expected 7 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 7


class StoreRecord(Record):
    """Store record: store_id, store_name, street, postal_code, city, state, latitude, longitude"""

    PARTS = 8

    def __init__(
        self,
        store_id,
        store_name,
        street,
        postal_code,
        city,
        state,
        latitude,
        longitude,
    ):
        self.store_id = store_id
        self.store_name = store_name
        self.street = street
        self.postal_code = postal_code
        self.city = city
        self.state = state
        self.latitude = latitude
        self.longitude = longitude

    def serialize(self):
        return f"{self.store_id}|{self.store_name}|{self.street}|{self.postal_code}|{self.city}|{self.state}|{self.latitude}|{self.longitude}"

    def get_type(self):
        return DatasetType.STORES

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid StoreRecord format: expected 8 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 8


class TransactionItemRecord(Record):
    """Transaction item record: transaction_id, item_id, quantity, unit_price, subtotal, created_at"""

    PARTS = 6

    def __init__(
        self, transaction_id, item_id, quantity, unit_price, subtotal, created_at
    ):
        self.transaction_id = transaction_id
        self.item_id = item_id
        self.quantity = quantity
        self.unit_price = unit_price
        self.subtotal = subtotal
        self.created_at = created_at

    def serialize(self):
        return f"{self.transaction_id}|{self.item_id}|{self.quantity}|{self.unit_price}|{self.subtotal}|{self.created_at}"

    def get_type(self):
        return DatasetType.TRANSACTION_ITEMS

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid TransactionItemRecord format: expected 6 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 6


class TransactionRecord(Record):
    """Transaction record: transaction_id, store_id, payment_method_id, voucher_id, user_id, original_amount, discount_applied, final_amount, created_at"""

    PARTS = 9

    def __init__(
        self,
        transaction_id,
        store_id,
        payment_method_id,
        voucher_id,
        user_id,
        original_amount,
        discount_applied,
        final_amount,
        created_at,
    ):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.payment_method_id = payment_method_id
        self.voucher_id = voucher_id
        self.user_id = user_id
        self.original_amount = original_amount
        self.discount_applied = discount_applied
        self.final_amount = final_amount
        self.created_at = created_at

    def serialize(self):
        return f"{self.transaction_id}|{self.store_id}|{self.payment_method_id}|{self.voucher_id}|{self.user_id}|{self.original_amount}|{self.discount_applied}|{self.final_amount}|{self.created_at}"

    def get_type(self):
        return DatasetType.TRANSACTIONS

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid TransactionRecord format: expected 9 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 9


class UserRecord(Record):
    """User record: user_id, gender, birthdate, registered_at"""

    PARTS = 4

    def __init__(self, user_id, gender, birthdate, registered_at):
        self.user_id = user_id
        self.gender = gender
        self.birthdate = birthdate
        self.registered_at = registered_at

    def serialize(self):
        return f"{self.user_id}|{self.gender}|{self.birthdate}|{self.registered_at}"

    def get_type(self):
        return DatasetType.USERS

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid UserRecord format: expected 4 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 4


class Q1Record(Record):
    """Q1 record: transaction_id, final_amount"""

    PARTS = 2

    def __init__(self, transaction_id, final_amount):
        self.transaction_id = transaction_id
        self.final_amount = final_amount

    def serialize(self):
        return f"{self.transaction_id}|{self.final_amount}"

    def get_type(self):
        return DatasetType.Q1

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid Q1Record format: expected 2 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 2


class Q2BestSellingRecord(Record):
    """Q2 best selling record: year_month, item_name, sellings_qty"""

    PARTS = 3

    def __init__(self, year_month, item_name, sellings_qty):
        self.year_month = year_month
        self.item_name = item_name
        self.sellings_qty = sellings_qty

    def serialize(self):
        return f"{self.year_month}|{self.item_name}|{self.sellings_qty}"

    def get_type(self):
        return DatasetType.Q2

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid Q2BestSellingRecord format: expected 3 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 3


class Q2MostProfitsRecord(Record):
    """Q2 most profits record: year_month, item_name, profit_sum"""

    PARTS = 3

    def __init__(self, year_month, item_name, profit_sum):
        self.year_month = year_month
        self.item_name = item_name
        self.profit_sum = profit_sum

    def serialize(self):
        return f"{self.year_month}|{self.item_name}|{self.profit_sum}"

    def get_type(self):
        return DatasetType.Q2

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid Q2MostProfitsRecord format: expected 3 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 3


class Q2Record(Record):
    """Q2 generic record for backwards compatibility - handles mixed quantity/subtotal records"""

    PARTS = 3

    def __init__(self, year_month, item_identifier, value):
        self.year_month = year_month
        self.item_identifier = item_identifier  # Can be item_id or item_name
        self.item_name = item_identifier  # Alias for compatibility
        self.value = value  # Could be sellings_qty or profit_sum

    def serialize(self):
        return f"{self.year_month}|{self.item_identifier}|{self.value}"

    def get_type(self):
        return DatasetType.Q2


class Q3Record(Record):
    """Q3 record: year_half_created_at, store_name, tpv"""

    PARTS = 3

    def __init__(self, year_half_created_at, store_name, tpv):
        self.year_half_created_at = year_half_created_at
        self.store_name = store_name
        self.tpv = tpv

    def serialize(self):
        return f"{self.year_half_created_at}|{self.store_name}|{self.tpv}"

    def get_type(self):
        return DatasetType.Q3

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid Q3Record format: expected 3 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 3


class Q4Record(Record):
    """Q4 record: store_name, purchases_qty, birthdate"""

    PARTS = 3

    def __init__(self, store_name, purchases_qty, birthdate):
        self.store_name = store_name
        self.purchases_qty = purchases_qty
        self.birthdate = birthdate

    def serialize(self):
        return f"{self.store_name}|{self.purchases_qty}|{self.birthdate}"

    def get_type(self):
        return DatasetType.Q4

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid Q4Record format: expected 3 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 3

class CleanupRecord(Record):
    """Cleanup record: client_id"""

    PARTS = 1

    def __init__(self, client_id):
        self.client_id = client_id

    def serialize(self):
        return f"{self.client_id}"

    def get_type(self):
        return DatasetType.CLEANUP

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid CleanupRecord format: expected 1 field, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 1


class BatchMessage:
    """Represents multiple records sent together for a specific dataset"""

    def __init__(self, dataset_type, batch_index, records, eof=False, client_id=""):
        self.type = MESSAGE_TYPE_BATCH
        self.dataset_type = dataset_type  # DatasetType enum value
        self.client_id = client_id  # Client identifier for routing responses
        self.batch_index = batch_index  # Batch index for this CSV file
        self.records = records  # List of Record objects
        self.eof = eof

    @classmethod
    def from_data(cls, data, has_client_id=True):
        """Parse batch message from custom protocol data
        
        Args:
            data: Raw bytes from protocol
            has_client_id: Whether the message includes client_id field
                          - True: messages from RabbitMQ (with client_id)
                          - False: messages from client (without client_id)
        """
        if len(data) < 2:
            raise ValueError("Invalid batch message: too short")

        if data[0] != MESSAGE_TYPE_BATCH:
            raise ValueError("Invalid batch message: not a batch message")

        dataset_type = data[1]
        content = data[2:].decode("utf-8")
        parts = content.split("|")

        if has_client_id:
            if len(parts) < 4:
                raise ValueError(
                    "Invalid batch message format: missing ClientID, BatchIndex, EOF and record count"
                )

            client_id = parts[0]
            batch_index = int(parts[1])
            eof = parts[2] == "1"
            record_count = int(parts[3])
            data_parts = parts[4:]  
        else:
            if len(parts) < 3:
                raise ValueError(
                    "Invalid batch message format: missing BatchIndex, EOF and record count"
                )

            client_id = ""  # Will be injected by connection-node
            batch_index = int(parts[0])
            eof = parts[1] == "1"
            record_count = int(parts[2])
            data_parts = parts[3:]  # Skip BatchIndex, EOF and record_count

        # Special handling for Q2 mixed records
        if dataset_type == DatasetType.Q2:
            logging.info(
                f"action: parse_q2_mixed_batch | first_record_count: {record_count} | total_fields: {len(data_parts)}"
            )
            records = _parse_q2_mixed_records(data_parts, record_count)
        else:
            # Standard parsing for other dataset types
            record_class = _get_record_class(dataset_type)
            fields_per_record = record_class.get_field_count()

            # Calculate how many complete records we can parse from ALL available data
            max_possible_records = len(data_parts) // fields_per_record

            # Parse ALL available complete records, not just the initial record_count
            records_to_parse = max_possible_records

            records = []
            for i in range(records_to_parse):
                start_idx = i * fields_per_record
                end_idx = start_idx + fields_per_record

                if end_idx <= len(data_parts):
                    record_fields = data_parts[start_idx:end_idx]

                    try:
                        record = record_class.from_parts(record_fields)
                        records.append(record)

                    except Exception as e:
                        logging.error(
                            f"action: create_record | index: {i} | error: {e} | fields: {record_fields}"
                        )

            # Log any remaining unparsed parts for standard records too
            remaining_parts = len(data_parts) % fields_per_record
            if remaining_parts > 0:
                unparsed_parts = data_parts[-(remaining_parts):]

        return cls(dataset_type, batch_index, records, eof, client_id)


class ResponseMessage:
    """Represents server response to client"""

    def __init__(self, success, error=None):
        self.type = MESSAGE_TYPE_RESPONSE
        self.success = success
        self.error = error


def _get_record_class(dataset_type):
    """Get the record class for a dataset type"""
    record_classes = {
        DatasetType.MENU_ITEMS: MenuItemRecord,
        DatasetType.STORES: StoreRecord,
        DatasetType.TRANSACTION_ITEMS: TransactionItemRecord,
        DatasetType.TRANSACTIONS: TransactionRecord,
        DatasetType.USERS: UserRecord,
        DatasetType.Q1: Q1Record,
        DatasetType.Q2: Q2Record,  # Uses generic Q2Record - special handling in BatchMessage.from_data()
        DatasetType.Q3: Q3Record,
        DatasetType.Q4: Q4Record,
    }

    record_class = record_classes.get(dataset_type)
    if not record_class:
        raise ValueError(f"Unknown dataset type: {dataset_type}")

    return record_class


def _parse_q2_mixed_records(data_parts, first_record_count):
    """
    Special parser for Q2 records with the format:
    count1|record1_data|count2|record2_data

    Where:
    - count1 = number of Q2BestSellingRecord (year_month|item_name|sellings_qty)
    - count2 = number of Q2MostProfitsRecord (year_month|item_name|profit_sum)

    Example: 2|2025-01|Flat White|156120|2024-01|Latte|155445|2|2024-01|Matcha Latte|3098440.00|2025-01|Matcha Latte|3104830.00
    """
    records = []
    fields_per_record = 3  # All Q2 records have 3 fields
    current_idx = 0

    # Parse first group (Q2BestSellingRecord - quantity records)
    best_selling_count = first_record_count

    for i in range(best_selling_count):
        if current_idx + fields_per_record <= len(data_parts):
            record_fields = data_parts[current_idx : current_idx + fields_per_record]
            year_month, item_name, sellings_qty = record_fields

            record = Q2BestSellingRecord(year_month, item_name, sellings_qty)
            records.append(record)
            current_idx += fields_per_record
        else:
            logging.error(f"action: insufficient_data_for_best_selling | index: {i}")
            break

    # Parse second group count and records (Q2MostProfitsRecord - profit records)
    if current_idx < len(data_parts):
        try:
            most_profits_count = int(data_parts[current_idx])
            current_idx += 1

            for i in range(most_profits_count):
                if current_idx + fields_per_record <= len(data_parts):
                    record_fields = data_parts[
                        current_idx : current_idx + fields_per_record
                    ]
                    year_month, item_name, profit_sum = record_fields

                    record = Q2MostProfitsRecord(year_month, item_name, profit_sum)
                    records.append(record)
                    current_idx += fields_per_record
                else:
                    logging.error(
                        f"action: insufficient_data_for_most_profits | index: {i}"
                    )
                    break

        except (ValueError, IndexError) as e:
            logging.error(
                f"action: parse_second_count_failed | error: {e} | current_idx: {current_idx}"
            )

    # Log any remaining unparsed data
    if current_idx < len(data_parts):
        remaining_data = data_parts[current_idx:]
        logging.warning(
            f"action: unparsed_data_remaining | remaining: {remaining_data}"
        )
    return records


class QueryReplyMessage:
    """Represents a query reply message from the replies_queue (different format from BatchMessage)"""

    def __init__(self, dataset_type, records, batch_index=0, eof=False, client_id=""):
        self.dataset_type = dataset_type
        self.client_id = client_id
        self.records = records
        self.batch_index = batch_index
        self.eof = eof

    @classmethod
    def from_data(cls, data: bytes):
        """Parse query reply message from replies_queue format with protocol header"""
        try:
            # Check for protocol header (first 2 bytes are message_type and dataset_type)
            if len(data) < 2:
                raise ValueError("Invalid reply message: too short")

            # Extract message type and dataset type from first 2 bytes
            message_type = data[0]  # Should be MESSAGE_TYPE_BATCH (1)
            dataset_type_byte = data[1]  # Dataset type as byte

            # Map dataset type byte to DatasetType constant
            dataset_type_map = {
                1: DatasetType.MENU_ITEMS,
                2: DatasetType.STORES,
                3: DatasetType.TRANSACTION_ITEMS,
                4: DatasetType.TRANSACTIONS,
                5: DatasetType.USERS,
                6: DatasetType.Q1,
                9: DatasetType.Q2,
                12: DatasetType.Q3,
                16: DatasetType.Q4,
            }

            dataset_type = dataset_type_map.get(dataset_type_byte)
            if not dataset_type:
                raise ValueError(f"Unknown dataset type byte: {dataset_type_byte}")

            # Decode the rest of the message (skip the 2 header bytes)
            content = data[2:].decode("utf-8")

            # Split content into parts
            parts = content.split("|")

            # The first part should be client_id, then batch_index, eof, record_count
            if len(parts) < 4:
                raise ValueError(
                    "Invalid reply message format: missing client_id, batch_index, EOF and record count"
                )

            client_id = parts[0]
            batch_index = int(parts[1])
            eof = parts[2] == "1"
            record_count = int(parts[3])

            # Get the data parts (everything after client_id, batch_index, eof, and record_count)
            data_parts = parts[4:]

            # Special handling for Q2 (mixed records)
            if dataset_type == DatasetType.Q2:
                records = _parse_q2_mixed_records(data_parts, record_count)
            else:
                record_class_map = {
                    DatasetType.Q1: Q1Record,
                    DatasetType.Q3: Q3Record,
                    DatasetType.Q4: Q4Record,
                }

                record_class = record_class_map.get(dataset_type)
                if not record_class:
                    raise ValueError(f"Unknown dataset type: {dataset_type}")

                fields_per_record_map = {
                    DatasetType.Q1: 2,  # Q1Record: transaction_id, final_amount
                    DatasetType.Q3: 3,  # Q3Record: year_half_created_at, store_name, tpv
                    DatasetType.Q4: 3,  # Q4Record: store_name, purchases_qty, birthdate
                }

                fields_per_record = fields_per_record_map[dataset_type]

                records = []
                for i in range(record_count):
                    start_idx = i * fields_per_record
                    end_idx = start_idx + fields_per_record

                    if end_idx <= len(data_parts):
                        record_fields = data_parts[start_idx:end_idx]
                        record = record_class(*record_fields)
                        records.append(record)
                    else:
                        logging.warning(
                            f"action: skip_reply_record | index: {i} | insufficient_data | needed: {end_idx} | available: {len(data_parts)}"
                        )

            return cls(dataset_type, records, batch_index, eof, client_id)

        except Exception as e:
            logging.error(
                f"action: query_reply_parsing_error | error: {e} | data_preview: {data[:100]}"
            )
            return None
