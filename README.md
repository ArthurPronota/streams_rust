# Streams в async на примере `tokio_stream`

## Что демонстрирует код

Пример показывает **асинхронный поток** (Stream) — аналог `Iterator`, но для асинхронного мира. Создаётся поток из чисел `[1, 2, 3, 4, 5]`, к нему применяется фильтр (только чётные), и результат читается в цикле `while let`.

## Разбор кода по частям

### 1. Импорт

```rust
use tokio_stream::StreamExt;
```

- `StreamExt` — **extension trait** (трейт расширения), который добавляет методы-адаптеры к любому типу, реализующему `Stream`.
- Аналогия: `Iterator` имеет `IteratorExt` в стандартной библиотеке, а `Stream` — `StreamExt` в `tokio_stream` или `futures`.

### 2. Создание потока

```rust
let mut s = tokio_stream::iter(vec![1, 2, 3, 4, 5])
    .filter(|x| x % 2 == 0);
```

- **`tokio_stream::iter(vec![1, 2, 3, 4, 5])`** — создаёт поток из вектора. Каждый элемент «выдаётся» по одному при вызове `next().await`. Это аналог `std::iter::iter()` для итераторов.
- **`.filter(|x| x % 2 == 0)`** — **адаптер**. Создаёт **новый** поток, который пропускает только чётные числа. Обратите внимание:
  - `filter` **ленивый** — он не выполняется сразу;
  - замыкание `|x| x % 2 == 0` вызывается **по мере вытягивания** элементов;
  - в async-версии замыкание может быть `async` (если нужно), но здесь — синхронное.
- **`let mut s`** — поток должен быть изменяемым, потому что `next()` требует `&mut self`.

### 3. Чтение потока

```rust
while let Some(v) = s.next().await {
    println!("{v}");
}
```

- **`s.next()`** — возвращает future, который резолвится в `Option<T>`:
  - `Some(v)` — есть следующий элемент;
  - `None` — поток исчерпан.
- **`.await`** — ждёт готовности следующего элемента. В отличие от `Iterator::next()`, здесь можно **приостановиться** (если поток асинхронный).
- **`while let Some(v) = ...`** — цикл, пока поток не вернёт `None`.
- **`println!("{v}")`** — печатает значение.

### 4. Вывод

```
2
4
```

Потому что фильтр `x % 2 == 0` пропускает только чётные: 2 и 4.

## Что такое Stream

**Stream** — это **асинхронный аналог `Iterator`**. Если `Iterator` синхронно возвращает элементы через `next() -> Option<T>`, то `Stream` возвращает их **асинхронно** через `poll_next() -> Poll<Option<T>>`.

### Сравнение

| Характеристика | `Iterator` | `Stream` |
|---|---|---|
| Метод | `next() -> Option<T>` | `poll_next() -> Poll<Option<T>>` |
| Синхронность | Синхронный | Асинхронный |
| Использование | `for x in iter` | `while let Some(x) = s.next().await` |
| Источник | Коллекции, диапазоны | Каналы, сокеты, файлы |
| Ленивость | Да | Да |
| Адаптеры | `map`, `filter`, `take`, ... | `map`, `filter`, `take`, `chunks`, `throttle`, ... |

### `Poll<Option<T>>`

`poll_next` возвращает `Poll`, который может быть:

- **`Poll::Ready(Some(v))`** — есть следующий элемент `v`.
- **`Poll::Ready(None)`** — поток завершён.
- **`Poll::Pending`** — данных пока нет, нужно подождать. Когда данные появятся, runtime разбудит задачу и вызовет `poll_next` снова.

Именно `Pending` делает `Stream` асинхронным: поток может «подождать», не блокируя поток ОС.

## Где применяются Streams

Streams идеально ложатся на **event-driven** архитектуры. Основные сценарии:

### 1. Обработка потока сообщений

```rust
let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(100);

while let Some(msg) = rx.recv().await {
    process(msg).await;
}
```

`Receiver` из `mpsc` — это Stream. Можно применять адаптеры:

```rust
rx.filter(|m| m.is_valid())
  .map(|m| m.parse())
  .for_each(|m| async { handle(m).await })
  .await;
```

### 2. События (event bus, UI, WebSocket)

```rust
while let Some(event) = ws_stream.next().await {
    match event {
        Event::Message(m) => handle_message(m).await,
        Event::Close => break,
    }
}
```

### 3. Чанки из сети

```rust
let mut stream = reqwest::get(url).await?.bytes_stream();

while let Some(chunk) = stream.next().await {
    let chunk = chunk?;
    process_chunk(&chunk);
}
```

Данные приходят **по частям** (chunks), и вы обрабатываете их по мере поступления, не дожидаясь всего ответа.

### 4. Строки из файла

```rust
use tokio::io::{AsyncBufReadExt, BufReader};

let file = tokio::fs::File::open("data.txt").await?;
let reader = BufReader::new(file);
let mut lines = reader.lines();

while let Some(line) = lines.next_line().await? {
    println!("{}", line);
}
```

### 5. Периодические события (таймеры)

```rust
use tokio::time::{interval, Duration};

let mut tick = interval(Duration::from_secs(1));

while let Some(_) = tick.next().await {
    println!("tick");
}
```

## Адаптеры Streams

Крейты `futures` и `tokio_stream` предоставляют богатый набор адаптеров:

| Адаптер | Что делает |
|---|---|
| `map` | Преобразует каждый элемент |
| `filter` | Пропускает элементы по предикату |
| `filter_map` | Фильтр + map в одном |
| `take` | Берёт только N первых элементов |
| `skip` | Пропускает N первых элементов |
| `chunks` | Группирует элементы в чанки по N |
| `throttle` | Ограничивает частоту элементов |
| `timeout` | Прерывает, если элемент не пришёл за время |
| `try_next` | Возвращает `Result` вместо `Option` (для потоков ошибок) |
| `merge` | Объединяет несколько потоков |
| `zip` | Соединяет два потока в пары |
| `buffered` | Обрабатывает N элементов параллельно, сохраняя порядок |
| `buffer_unordered` | Обрабатывает N элементов параллельно, без порядка |
| `for_each` | Применяет async-функцию к каждому элементу |
| `fold` | Свёртка (аналог `Iterator::fold`) |
| `collect` | Собирает все элементы в коллекцию |

### Пример с `chunks`

```rust
let mut s = tokio_stream::iter(0..10)
    .chunks(3);

while let Some(chunk) = s.next().await {
    println!("{:?}", chunk);
}
// [0, 1, 2]
// [3, 4, 5]
// [6, 7, 8]
// [9]
```

### Пример с `throttle`

```rust
use tokio::time::Duration;

let mut s = tokio_stream::iter(0..5)
    .throttle(Duration::from_millis(500));

while let Some(v) = s.next().await {
    println!("{}", v);   // по одному каждые 500 мс
}
```

## Ленивость Streams

Streams **ленивы** — как и `Iterator`. Адаптеры `map`, `filter` и т.д. **не выполняются** до тех пор, пока вы не начнёте вытягивать элементы через `next().await`.

```rust
let s = tokio_stream::iter(vec![1, 2, 3])
    .map(|x| {
        println!("map: {}", x);   // не выполнится, пока нет next()
        x * 2
    });
// Здесь ничего не напечатано

s.next().await;   // только теперь напечатается "map: 1"
```

Каждый шаг **ленивый** — вычисляется ровно столько, сколько нужно для получения следующего элемента. Это ключевое свойство для работы с **бесконечными** или **большими** потоками данных: вы не загружаете всё в память, а обрабатываете по мере поступления.

## Разница между `Iterator` и `Stream`

### Синхронный `Iterator`

```rust
let v: Vec<i32> = (0..5).filter(|x| x % 2 == 0).collect();
// [0, 2, 4]
```

- `next()` возвращает `Option<T>` **немедленно**.
- Не может «подождать» — данные уже в памяти.

### Асинхронный `Stream`

```rust
let mut s = tokio_stream::iter(vec![0, 1, 2, 3, 4])
    .filter(|x| x % 2 == 0);

while let Some(v) = s.next().await {
    println!("{}", v);
}
// 0
// 2
// 4
```

- `next().await` может **приостановиться** (например, если данные приходят из сети).
- Работает с **источниками, которые не готовы сразу**: сокеты, каналы, файлы, таймеры.

## Как Stream интегрируется с async/await

Stream — это **Future-подобная** абстракция, но с несколькими значениями. Rust не позволяет напрямую использовать `for` с `Stream` (как с `Iterator`), поэтому есть два подхода:

### 1. `while let Some(v) = s.next().await`

```rust
while let Some(v) = s.next().await {
    // обработать v
}
```

### 2. `StreamExt::for_each`

```rust
s.for_each(|v| async move {
    println!("{}", v);
}).await;
```

`for_each` принимает **async-замыкание**, что позволяет асинхронно обрабатывать каждый элемент.

## Практический пример: обработка WebSocket

```rust
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;

#[tokio::main]
async fn main() {
    let (ws_stream, _) = connect_async("wss://echo.websocket.org").await.unwrap();
    let (mut write, mut read) = ws_stream.split();

    // Отправляем сообщение
    write.send("Hello".into()).await.unwrap();

    // Читаем поток сообщений
    while let Some(msg) = read.next().await {
        match msg {
            Ok(m) => println!("Received: {:?}", m),
            Err(e) => eprintln!("Error: {}", e),
        }
    }
}
```

Здесь `read` — это Stream. `read.next().await` ждёт следующее сообщение из сокета.

## Практический пример: обработка потока с ограничением параллелизма

```rust
use futures::stream::{self, StreamExt};

#[tokio::main]
async fn main() {
    stream::iter(1..=100)
        .map(|i| async move {
            // имитация работы
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            i * 2
        })
        .buffer_unordered(10)   // не более 10 параллельно
        .for_each(|result| async move {
            println!("{}", result);
        })
        .await;
}
```

`buffer_unordered(10)` — backpressure: не более 10 futures выполняются одновременно.

## Итог

| Аспект | Описание |
|---|---|
| **Stream** | Асинхронный аналог `Iterator` |
| **Метод** | `poll_next() -> Poll<Option<T>>` |
| **Чтение** | `while let Some(v) = s.next().await` |
| **Ленивость** | Да, как у `Iterator` |
| **Применение** | Каналы, сокеты, файлы, таймеры, WebSocket, события |
| **Адаптеры** | `map`, `filter`, `chunks`, `throttle`, `buffer_unordered`, ... |
| **Крейты** | `futures`, `tokio_stream`, `async-stream` |
| **Ключевое отличие от Iterator** | Может «подождать» данные (`Pending`) |
| **Backpressure** | Через `buffer_unordered`, `chunks`, bounded-каналы |
| **Идиоматичность** | Хорошо ложится на event-driven архитектуру |

В вашем примере: `tokio_stream::iter` создаёт поток из вектора, `.filter` пропускает чётные, `while let Some(v) = s.next().await` читает по одному. Это **минимальный пример**, демонстрирующий базовую механику Streams.
