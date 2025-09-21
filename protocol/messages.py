# Message types (matching Go client)
MESSAGE_TYPE_BET = 1
MESSAGE_TYPE_BATCH = 2
MESSAGE_TYPE_RESPONSE = 3
MESSAGE_TYPE_GET_WINNERS = 4


class BetMessage:
    """Represents a betting request from client"""

    def __init__(self, nombre, apellido, documento, nacimiento, numero):
        self.type = MESSAGE_TYPE_BET
        self.nombre = nombre
        self.apellido = apellido
        self.documento = documento
        self.nacimiento = nacimiento
        self.numero = numero

    @classmethod
    def from_data(cls, data):
        """Parse bet message from custom protocol data"""
        if len(data) < 1 or data[0] != MESSAGE_TYPE_BET:
            raise ValueError("Invalid bet message")

        content = data[1:].decode("utf-8")
        parts = content.split("|")

        if len(parts) < 5:
            raise ValueError("Invalid bet message format")

        return cls(
            nombre=parts[0],
            apellido=parts[1],
            documento=parts[2],
            nacimiento=parts[3],
            numero=int(parts[4]),
        )


class BatchMessage:
    """Represents multiple bets sent together"""

    def __init__(self, agency, bets, eof=False):
        self.type = MESSAGE_TYPE_BATCH
        self.agency = agency  # Agency number (1-5)
        self.bets = bets  # List of BetMessage objects
        self.eof = eof

    @classmethod
    def from_data(cls, data):
        """Parse batch message from custom protocol data"""
        if len(data) < 1 or data[0] != MESSAGE_TYPE_BATCH:
            raise ValueError("Invalid batch message")

        content = data[1:].decode("utf-8")
        parts = content.split("|")

        if len(parts) < 3:
            raise ValueError("Invalid batch message format")

        agency = int(parts[0])
        eof = parts[1] == "1"
        bet_count = int(parts[2])

        bets = []
        idx = 3
        for _ in range(bet_count):
            if idx + 4 >= len(parts):
                break
            bet = BetMessage(
                nombre=parts[idx],
                apellido=parts[idx + 1],
                documento=parts[idx + 2],
                nacimiento=parts[idx + 3],
                numero=int(parts[idx + 4]),
            )
            bets.append(bet)
            idx += 5

        return cls(agency, bets, eof)


class GetWinnersMessage:
    """Represents a request to get winners for an agency"""

    def __init__(self, agency):
        self.type = MESSAGE_TYPE_GET_WINNERS
        self.agency = agency

    @classmethod
    def from_data(cls, data):
        """Parse get winners message from custom protocol data"""
        if len(data) < 1 or data[0] != MESSAGE_TYPE_GET_WINNERS:
            raise ValueError("Invalid get winners message")

        content = data[1:].decode("utf-8")
        agency = int(content)
        return cls(agency)


class ResponseMessage:
    """Represents server response to client"""

    def __init__(self, success, error=None, winners=None):
        self.type = MESSAGE_TYPE_RESPONSE
        self.success = success
        self.error = error
        self.winners = winners or []