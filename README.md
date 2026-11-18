# Protocol orientat pe conexiune peste UDP

## 1. Prezentare generala

Aceasta tema implementeaza un protocol fiabil, orientat pe conexiune, construit
peste UDP. Scopul este transferul corect al unui flux de octeti intre un client si un server, in conditii de pierdere de pachete, latenta si viteza de transmisie arbitrare.

In acest scenariu clientul este transmitatorul (sender), iar serverul este
receptorul (receiver): clientul trimite datele, iar serverul le receptioneaza si
raspunde doar cu segmente de control. Protocolul foloseste o fereastra glisanta
atat la sender, cat si la receiver, si dimensioneaza fereastra in functie de
caracteristicile legaturii (viteza si latenta).

## 2. Structura segmentelor

Protocolul foloseste doua tipuri de header, conform specificatiei temei.

Header pentru segmente de date (`poli_tcp_data_hdr`):

- `protocol_id` - identificator de protocol (valoarea 42)
- `conn_id` - identificator de conexiune
- `type` - tipul segmentului (TYPE_MESSAGE pentru date)
- `seq_num` - numarul de secventa al segmentului
- `len` - dimensiunea payload-ului

Header pentru segmente de control (`poli_tcp_ctrl_hdr`):

- `protocol_id`, `conn_id`, `type` - aceleasi roluri
- `ack_num` - numarul de secventa confirmat (ACK cumulativ)
- `recv_window` - spatiul liber din buffer-ul receptorului (flow control)

Tipuri de segmente folosite: TYPE_SYN, TYPE_SYN_ACK, TYPE_ACK, TYPE_MESSAGE.

### Dimensiunea payload-ului

`MAX_DATA_SIZE` este setat la 1400 de octeti. Valoarea este aleasa pentru a sta
sub MTU-ul tipic de 1500 de octeti chiar si dupa adaugarea header-elor IP (20),
UDP (8) si al protocolului nostru. Astfel datagrama UDP nu este fragmentata la
nivel IP.

## 3. Stabilirea conexiunii (three-way handshake)

Conexiunea se stabileste printr-un handshake in trei pasi:

1. Clientul trimite un segment SYN catre server pe portul 8032.
2. Serverul aloca un socket nou si un port dedicat acelei conexiuni (obtinut de
   la sistemul de operare prin `bind` cu port 0 si extras cu `getsockname`).
   Trimite inapoi un SYN-ACK care contine in payload portul ales pentru
   comunicarea cu acel client.
3. Clientul extrage portul din SYN-ACK, isi actualizeaza adresa destinatie si
   trimite un ACK final pe noul port.

Astfel clientul foloseste un singur socket atat pentru handshake, cat si pentru
date, iar serverul are un socket dedicat acceptarii de conexiuni (8032) plus cate
un socket per client, comportament similar cu `accept`.

Atat SYN-ul (la client), cat si SYN-ACK-ul (la server) sunt retransmise pe timeout
daca raspunsul nu soseste, pentru a tolera pierderea pachetelor de control in
timpul handshake-ului.

## 4. Transferul de date

### 4.1 Fereastra glisanta la sender

La sender, datele primite din aplicatie prin `send_data` sunt impachetate in
segmente si puse intr-o coada de backlog (`send_queue`). Thread-ul de protocol
(`sender_handler`) muta segmente din backlog in fereastra de transmisie
(`window`) si le trimite pe retea, atata timp cat fereastra are loc.

Starea ferestrei este descrisa de:

- `send_base` - cel mai mic numar de secventa neconfirmat
- `next_seq_num` - urmatorul numar de secventa care va fi atribuit
- `window` - segmentele trimise, dar inca neconfirmate (inflight)

Dimensiunea ferestrei se calculeaza pe baza Bandwidth-Delay Product (BDP):

```
BDP (octeti) = (viteza in octeti/secunda) * RTT
RTT = 2 * delay
sender_window = (BDP / MAX_SEGMENT_SIZE) * 2
```

### 4.2 ACK cumulativ

Receptorul foloseste ACK cumulativ: in loc sa confirme fiecare segment separat,
trimite un singur ACK care confirma toate segmentele primite in ordine pana la un
anumit numar de secventa. Campul `ack_num` contine cel mai mare numar de secventa
primit in ordine (`expected_recv_seq - 1`).

Avantajul acestei implementari este: daca un ACK se pierde, urmatorul ACK il acopera automat,
deoarece confirma cumulativ tot ce s-a primit pana atunci. La primirea unui ACK
nou, sender-ul avanseaza `send_base` si scoate din fereastra toate segmentele cu
numar de secventa mai mic sau egal cu cel confirmat.

### 4.3 Recuperarea pierderilor

Recuperarea se face prin doua mecanisme:

- Fast Retransmit: cand un segment se pierde, receptorul reconfirma acelasi numar
  cumulativ pentru fiecare segment ulterior. Sender-ul vede aceste confirmari ca
  ACK-uri duplicate (`ack_num < send_base`). Dupa 3 ACK-uri duplicate
  (`DUP_ACKS_THRESHOLD`), retransmite imediat primul segment neconfirmat, fara sa
  astepte expirarea timer-ului.
- Retransmisie pe timeout: daca nu se primeste niciun progres, la expirarea
  timer-ului se retransmite doar primul segment neconfirmat din fereastra (golul),
  nu toata fereastra.

Timer-ul este implementat per conexiune folosind `timerfd`, conform cerintei
temei, nu per segment.

### 4.4 Selective Repeat la receiver

Receptorul implementeaza Selective Repeat: accepta si pastreaza segmentele sosite
in dezordine, in loc sa le arunce. Structurile folosite sunt:

- `expected_recv_seq` - numarul de secventa pe care receptorul il asteapta in
  continuare pentru a mentine ordinea stricta
- `recv_queue` - segmentele gata de a fi citite de aplicatie, in ordine
- `early_packets` - zona de asteptare pentru segmentele sosite mai devreme decat
  erau asteptate (cheia este numarul de secventa)

Cand soseste segmentul asteptat, acesta este pus in `recv_queue` si se verifica
daca segmente din `early_packets` umplu golul urmator; daca da, sunt mutate la
randul lor in `recv_queue`. Un segment sosit prea devreme este pus in
`early_packets`. Un segment duplicat (numar de secventa mai mic decat cel
asteptat) este ignorat, dar se trimite oricum un ACK cumulativ ca sender-ul sa
poata avansa.

### 4.5 Flow control

Receptorul anunta in fiecare ACK, prin campul `recv_window`, cate pachete mai
incap in buffer-ul sau (`recv_window` global minus segmentele deja stocate in
`recv_queue` si `early_packets`). Sender-ul salveaza aceasta valoare in
`peer_recv_window` si limiteaza fereastra efectiva la minimul dintre fereastra de
congestie si spatiul liber raportat de receptor:

```
eff_window = min(per_conn_window, peer_recv_window)
```

In plus, fereastra este redeschisa activ din `recv_data`: in momentul in care
aplicatia citeste date si elibereaza spatiu in buffer, receptorul trimite imediat
un ACK cu valoarea actualizata a ferestrei. Acest lucru anunta sender-ul ca poate
relua transmisia si previne blocajul descris mai sus.

### 4.6 Functionarea cu mai multe conexiuni

Fiecare conexiune foloseste intreaga fereastra de transmisie. Controlul efectiv al
ratei pe legatura este asigurat de fereastra anuntata de receptor
si de mecanismele de recuperare a pierderilor.

La server, fiecare client are un socket dedicat, identificat la receptie prin
intermediul tabelei de conexiuni. Astfel mai multe transferuri pot fi procesate
in paralel de catre `receiver_handler`.

## 5. Arhitectura cu thread-uri

Implementarea foloseste doua thread-uri:

- Thread-ul principal apartine aplicatiei (client.cpp / server.cpp) si apeleaza
  `send_data` / `recv_data`.
- Un al doilea thread, dedicat protocolului (`sender_handler` la client,
  `receiver_handler` la server), ruleaza in fundal si se ocupa de transmisia
  efectiva, retransmisii, procesarea ACK-urilor si reordonarea segmentelor.
