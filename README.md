# Coffee Shop Analysis - Connection Node

Este es el nodo de conexión para el sistema de análisis de cafeterías. Actúa handler que recibe datasets de clientes y los rutea a las colas apropiadas para procesamiento, además de reenviar las respuestas de las queries de vuelta al cliente.

## Funcionalidad

### Recepción de Datasets

El connection node recibe 5 tipos de datasets del cliente:

- **Menu Items** (DatasetType: 1): Información de productos del menú
- **Stores** (DatasetType: 2): Información de tiendas
- **Transaction Items** (DatasetType: 3): Items individuales de transacciones
- **Transactions** (DatasetType: 4): Transacciones completas
- **Users** (DatasetType: 5): Información de usuarios

### Ruteo a Queues

Los datasets se rutean a diferentes colas según su tipo:

- **Stores** → `joiner_n_queue_stores`
- **Transaction Items** y **Transactions** → `transactions_queue`
- **Users** → `joiner_n_queue_users`
- **Menu Items** → `joiner_n_queue_menu_items`

### Respuestas de Queries

El nodo lee de la cola `replies_queue` las respuestas procesadas (Q1-Q4) y las reenvía al cliente:

- **Q1** (DatasetType: 10): transaction_id, final_amount
- **Q2** (DatasetType: 11): year_month_created_at, item_name, sellings_qty
- **Q3** (DatasetType: 12): year_half_created_at, store_name, tpv
- **Q4** (DatasetType: 13): store_name, birthdate

## Como correr

Para levantar el connection node:
```sh
make docker-compose-up
```

Para limpiar los contenedores:
```sh
make docker-compose-down
```
