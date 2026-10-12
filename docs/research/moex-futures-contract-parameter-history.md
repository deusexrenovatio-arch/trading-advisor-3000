# История параметров фьючерсов MOEX: `MINSTEP`, `LOTVOLUME`, стоимость шага

Дата проверки: 2026-07-10. Интервал TA3000: 2022-06-17 — 2026-07-09.

## Дополнение 2026-09-29: расширенный raw

В изолированном staging `a7f622c5-c33d-46b3-bea9-507944613889` проверены
511 контрактов: все 30 780 287 канонических свечей укладываются в прежнюю
ценовую сетку. До 17.06.2022 доступны 24 892 ликвидные строки оборота по
11 семействам. Максимальный p95 ошибки стоимости шага по семейству —
0.023784% (SILV), ниже прежнего порога 0.1%. Это отдельная проверка реального
оборота, а не сравнение заполненных параметров с тем же справочником.

Добавлены ограниченные режимы `*-pre2022-v1`: с 27.01.2020 для BR, GOLD,
SILV, PLD, PLT, RTS, MIX, MXI; с 03.02.2020 для NG, с 25.05.2021 для SPYF,
с 28.02.2022 для RGBI — с [официальной даты запуска](https://www.moex.com/n41302),
включая settlement-строки до первых ликвидных торгов 23.03.2022.
Конец каждого — 16.06.2022. Старые режимы сохранены.
На даты до этих границ перенос параметров не разрешён этой проверкой.

У 20 архивных карточек ISS SILV/PLD `LOTSIZE` противоречит фактическому
обороту и спецификации: указан старый лот 100/10 вместо 10/1. Официальное
[уведомление MOEX от 01.09.2016](https://www.moex.com/n13664) вводит меньшие
лоты с 19.09.2016; [спецификация 2018 года](https://www.moex.com/ru/documents/9430)
и независимый оборот всех 20 контрактов подтверждают 10/1. В подготовленной
таблице исходные карточки и исключения сохранены; их ошибочный лот не принят.

Исходные 112 205 строк истории не меняются. Подготовленная Delta содержит
заполненные шаг, лот и экспирацию, исходный payload и settlement, а также
`enrichment_provenance_json`. 2 271 строка получает официальные STEPPRICE и
INITIALMARGIN из прежнего raw v134 только по точному ключу SECID/assetcode/дата,
не позднее 28.07.2026. Эти наблюдения не переносятся на прошлые даты.

У свежего raw есть пары USD/JPY, USD/CNY и другие кросс-курсы. Для расчёта
в RUB допускаются только пары `*/RUB`. Первичная проверка, смешавшая пары,
признана недействительной и сохранена отдельно. На периоде после 16.06.2026
проверка восстановленной стоимости шага по обороту превышает порог для части
семейств; режимы на этот интервал не расширялись. Используются датированные
официальные наблюдения, когда они есть; пропуски остаются ограничением.

Доказательства в `D:/TA-3000-rebuild/staging/ta3000-5y-20260729/specification-enrichment/a7f622c5-c33d-46b3-bea9-507944613889`:

- `audit/enrichment-report.json`, SHA256 `2a968ae6ca4c97536b4ba6581c983d76a87b0bb8dfcbd14ed614a74b78609c89`;
- `audit/price-grid-report.json`, SHA256 `e1e0868a657726126c04eafe8ea0379a448a2ed8d564a42093b40cbd1c41166c`;
- `sources/precious-metals-2018.docx`, SHA256 `00906740b4d022538a5bd3467565dadc8202073165945806d39a1ad9125f6a8a`.

Это завершение обогащения спецификаций в staging. Экономика ценовой истории
2018–2019 всё ещё требует более ранних settlement/FX/RMS входов; готовность
всего периода и публикация current из этой проверки не следуют.

## Вывод

В authoritative history нет признаков изменения `MINSTEP` или `LOTVOLUME` внутри одного `contract_id`: 13 семейств, 370 контрактов, один наблюдаемый режим `(MINSTEP, LOTVOLUME)` на `assetcode`. Для текущего backfill эти два поля можно считать постоянными внутри контракта.

Но вычислять стоимость шага как `MINSTEP × LOTVOLUME` нельзя. `LOTVOLUME` — канонический проверяемый параметр спецификации, а не универсальный денежный коэффициент. Это особенно видно на `MXI`, `RTS`, `NASD`.

Корректная модель:

```text
STEPPRICE_RUB    = tick_value_quote × FX(quote_currency → RUB)
```

Для рублёвых контрактов `FX=1`. Для USD-linked контрактов нужен точный USD/RUB соответствующей клиринговой сессии.

## Параметры 13 исторических семейств

| `assetcode` | `LOTVOLUME` / лот | `MINSTEP` | `tick_value_quote` | режим | официальный источник |
|---|---:|---:|---:|---|---|
| `BR` | 10 баррелей | 0.01 USD | 0.1 USD | USD-linked | [MOEX 2022](https://www.moex.com/files/4a7xfjnry1fzcxaeq5yazr7bcx), [текущие параметры](https://www.moex.com/ru/derivatives/commodity/oil/) |
| `GOLD` | 1 тр. унция | 0.1 USD | 0.1 USD | USD-linked | [MOEX 2022](https://www.moex.com/files/4a7xfjnry1fzcxaeq5yazr7bcx), [архив спецификаций и списков параметров](https://www.moex.com/ru/documents/9430) |
| `SILV` | 10 тр. унций | 0.01 USD | 0.1 USD | USD-linked | [MOEX 2022](https://www.moex.com/files/4a7xfjnry1fzcxaeq5yazr7bcx), [текущий список параметров](https://www.moex.com/files/4hdwgxm5qr4fnv3z0my26szgx7/4yqk4q0bkfqnn3am7m0df54p5j) |
| `PLD` | 1 тр. унция | 0.01 USD | 0.01 USD | USD-linked | [текущий список параметров MOEX](https://www.moex.com/files/4hdwgxm5qr4fnv3z0my26szgx7/4yqk4q0bkfqnn3am7m0df54p5j) |
| `PLT` | 1 тр. унция | 0.1 USD | 0.1 USD | USD-linked | [MOEX 2022](https://www.moex.com/files/4a7xfjnry1fzcxaeq5yazr7bcx), [текущий список параметров](https://www.moex.com/files/4hdwgxm5qr4fnv3z0my26szgx7/4yqk4q0bkfqnn3am7m0df54p5j) |
| `NG` | 100 MMBtu | 0.001 USD | 0.1 USD | USD-linked | [MOEX Productbook 2022](https://www.moex.com/files/4a7xfjnry1fzcxaeq5yazr7bcx), [текущие параметры](https://www.moex.com/msn/ru-futng) |
| `SPYF` | 1 пай | 0.01 USD | 0.01 USD | USD-linked | [MOEX 2022](https://www.moex.com/files/4a7xfjnry1fzcxaeq5yazr7bcx), [текущие параметры](https://www.moex.com/s3810) |
| `NASD` | 41 пай | 1 USD | 0.01 USD | USD-linked; цена уже за лот | [запуск 06.09.2022](https://www.moex.com/n51134), [текущие параметры](https://www.moex.com/s3810) |
| `RTS` | 1 контракт | 10 пунктов | 0.2 USD | USD-linked | [MOEX 2022](https://www.moex.com/files/4a7xfjnry1fzcxaeq5yazr7bcx), [текущие параметры индексов](https://www.moex.com/ru/derivatives/equity/indices/) |
| `MIX` | 1 контракт | 25 пунктов | 25 RUB | фиксированный RUB | [MOEX 2022](https://www.moex.com/files/4a7xfjnry1fzcxaeq5yazr7bcx), [текущие параметры индексов](https://www.moex.com/ru/derivatives/equity/indices/) |
| `MXI` | 1 контракт | 0.05 пункта | 0.5 RUB | фиксированный RUB | [архив редакций](https://www.moex.com/ru/documents/10061), [текущие параметры индексов](https://www.moex.com/ru/derivatives/equity/indices/) |
| `RGBI` | 1 контракт | 1 пункт | 1 RUB | фиксированный RUB | [запуск 28.02.2022](https://www.moex.com/n41302), [текущие параметры](https://www.moex.com/ru/rgbi) |
| `WHEAT` | 1 тонна | 10 RUB | 10 RUB | фиксированный RUB | [редакции 2022–2026](https://www.moex.com/ru/documents/24522), [редакция 31.08.2022](https://www.moex.com/files/4fshwvhb21ks6sagtjje5bm9wq), [текущая редакция](https://www.moex.com/files/4z3c37jjyaensw41xrgj41qrwh) |

`PLD` и `PLT` в текущей локальной классификации ошибочно помечены как RUB: официальная спецификация задаёт котировку и стоимость шага в USD. Их `STEPPRICE_RUB` обязан изменяться вместе с USD/RUB.

## Что проверено в локальной Delta

Источники:

- `D:/TA3000-data/trading-advisor-3000-nightly/raw/economics/raw_moex_contract_securities.delta`;
- `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current/raw_moex_history.delta`;
- официальный [MOEX ISS history](https://iss.moex.com/iss/history/engines/futures/markets/forts/boards/RFUD/securities.json) и [текущий securities snapshot](https://iss.moex.com/iss/engines/futures/markets/forts/securities.json).

Результат локальной проверки:

- 13 семейств, 370 `contract_id`; на каждом `assetcode` ровно один сохранённый режим `(MINSTEP, LOTVOLUME)`;
- 292 истёкших контракта: GCD наблюдавшейся OHLC-сетки точно равен `MINSTEP`, значений вне сетки нет;
- для тех же 292 контрактов `LOTVOLUME` совпадает с официальным `LOTSIZE` в MOEX ISS `description`, расхождений нет;
- оставшиеся 78 контрактов покрыты текущим официальным snapshot MOEX;
- на 61 844 ликвидных строках независимо рассчитан рублёвый `STEPPRICE` через `VALUE / (VOLUME × WAPRICE) × MINSTEP`; после нормализации USD-linked семейств точным USD/RUB получены значения из таблицы, максимальный построчный `p95` по семейству — 0.067% (`PLD`), ниже fail-closed порога 0.1%.

GCD-сверка сама по себе не доказывает отсутствие меньшего, но ни разу не использованного шага. Этот риск закрывается совпадением с официальными спецификациями и текущим snapshot.

## Серия контракта и смена спецификации — разные вещи

Новый срок исполнения (`BRX2` → `BRZ2`, `RIU2` → `RIZ2`) не означает новый режим спецификации: меняется `contract_id`, но семейные параметры сохраняются.

Стандартный и mini/micro-контракт — не смена режима одного семейства, а отдельные продукты и `assetcode`: `BR`/`BRM`, `NG`/`NGM`, `RTS`/`RTSM`, `SILV`/`SILVM`. MOEX прямо показывает разные лоты и стоимости шага для пар [`BR`/`BRM`](https://www.moex.com/ru/derivatives/commodity/oil/) и [`NG`/`NGM`](https://www.moex.com/msn/ru-futng).

Нельзя также склеивать разные спецификации по похожему базовому активу: `WHEAT` — расчётный контракт на индекс пшеницы с лотом 1 тонна; старый `WH4` — поставочный контракт на 25 тонн. Это разные коды и документы ([MOEX Productbook 2022](https://www.moex.com/files/4a7xfjnry1fzcxaeq5yazr7bcx), [архив `WHEAT`](https://www.moex.com/ru/documents/24522)).

## Менялись ли параметры во времени

Официальные библиотеки фиксируют множество редакций спецификаций: [`RTS`](https://www.moex.com/en/documents/4856), [`MIX`](https://www.moex.com/en/documents/4869), [`MXI`](https://www.moex.com/ru/documents/10061), [`RGBI`](https://www.moex.com/en/documents/24083), [драгоценные металлы](https://www.moex.com/ru/documents/9430), [энергоносители](https://www.moex.com/ru/documents/26811), [`WHEAT`](https://www.moex.com/ru/documents/24522).

Редакция документа не равна изменению `MINSTEP`/лота. Сравнение параметров MOEX 2022 с действующими параметрами и локальная посерийная проверка не выявили изменений этих двух полей на интервале TA3000. У `WHEAT` дополнительно напрямую сравнены первая редакция от 31.08.2022 и текущая: в обеих `MINSTEP=10 RUB`, стоимость шага `10 RUB`, лот `1 тонна`.

При этом спецификация позволяет Бирже вводить новую редакцию и распространять изменения на уже существующие обязательства. Поэтому постоянство следует считать доказанным фактом нашего интервала, а не вечным свойством `assetcode`.

## Требуемая модель данных

Хранить таблицу режимов спецификации с ключом:

```text
(assetcode, effective_from, effective_to)
```

Минимальные поля:

```text
quote_currency
min_step
lot_volume
tick_value_quote
source_document_id
source_document_hash
```

`contract_id` наследует режим по дате. Если MOEX изменит условия во время жизни серии, появится новая effective-dated строка; исторические факты не переписываются.

Для текущего backfill допустим один режим на каждое из 13 семейств. При nightly ingest обязательны проверки:

1. новый `(MINSTEP, LOTVOLUME, STEPPRICE)` против активного режима;
2. новый режим только с официальным MOEX provenance;
3. `STEPPRICE_RUB` против `tick_value_quote × FX`;
4. `MINSTEP` и `LOTVOLUME` должны точно совпадать с активным режимом;
5. запрет вывода `tick_value_quote` из `LOTVOLUME`.

## Историческое ГО

Официальное `INITIALMARGIN` сохраняется без корректировок и всегда имеет приоритет. Если в исторической строке официального ГО нет:

```text
factor(assetcode, maturity_rank) =
    official_initial_margin / margin_radius_adjusted
    на последнюю дату с официальным полным срезом

historical_margin = margin_radius_adjusted × factor
```

Если сегодняшнего `maturity_rank` нет, используется ближайший доступный rank того же `assetcode`; при равной дистанции — меньший rank. Межактивного и глобального fallback нет. Дополнительный buffer запрещён: `margin_buffer_pct=0`, `margin_required_estimate=margin_required_no_buffer`.

В canonical Delta явно сохраняются `margin_calibration_factor`, `margin_calibration_as_of_date`, `margin_calibration_rank`, `margin_calibration_source`. Строки различаются по качеству: `official_initial_margin`, `calibrated_asset_rank`, `calibrated_nearest_rank`, `unavailable`.

Обратная проверка коэффициентов среза 2026-07-09 на официальных данных 2026-06-17 — 2026-07-08 для 13 исторических семейств: медианная абсолютная ошибка 1.06%, `p95=6.16%`, 98.04% строк в пределах 10%. На 78 115 более ранних строках точный rank покрывает 84.39%, ближайший rank — 15.61%, неподдержанных `assetcode` нет. Полные четыре года проверить невозможно: бесплатное официальное ГО в локальном источнике начинается 2026-06-17.
## Early input extension, 29 September 2026

Staging evidence: `early-economics/17ab91c2-f13b-48cc-9be2-330312fa4a9a`.
The native acquisition added 6,844 settlement records for 43 existing contracts,
15,954 FX records and 32,555 RMS records. Original source payloads are preserved.
Prepared specifications now contain 119,049 rows. This is staged evidence;
full historical margin coverage is a separate check.

The eight `pre2020-v1` regimes stop on 26 January 2020 and do not change the
existing 2020+ intervals or their rule IDs:

| Asset | First source date | Turnover p95 relative error |
|---|---|---:|
| BR | 2018-06-26 | 0.00008267 |
| GOLD | 2019-03-07 | 0.00003310 |
| SILV | 2019-03-07 | 0.00027815 |
| PLD | 2019-06-07 | 0.00000904 |
| PLT | 2019-06-07 | 0.00005195 |
| RTS | 2018-01-10 | 0.00000599 |
| MIX | 2019-03-07 | 0.00000171 |
| MXI | 2019-03-07 | 0.00000165 |

All eight profiles satisfy the unchanged 0.001 validation limit. Bounds begin at
the acquired settlement history, which can precede the first liquid trade.
Evidence combines the 2018 metals parameter list, the RTS specification effective
2 August 2017, MIX/MXI specifications effective 20 February 2018, and the
[MOEX Brent presentation, April 2018 parameters, page 4](https://www.moex.com/files/4bb5dvk3zd59p0nnca9zskcex8).
The independent turnover audit and prior full price-grid proof agree with them.

ISS RMS returns no rows for 12 January or 21 May 2018; its first available
snapshot in this acquisition is 22 May 2018. Earlier MR1 is not backfilled.
Launch-day RMS observations dated the following day may be used only when their
explicit publication timestamp precedes or equals the economics interval start.
The prior dated observation retains priority. UTC-normalized `update_time` and
Moscow-local native `updatetime` are distinguished; missing/late timestamps fail
closed. Selection and publication time are recorded in economics provenance.
