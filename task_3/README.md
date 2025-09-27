# Výkonnostný problém  v SQL transformácii 3:

Na základe obrázka je tu zhrnutie problému a jeho riešení.

Graf ukazuje, že trvanie úlohy neustále rastie, z približne 1,5 hodiny dňa 11. novembra na 2 hodiny a 44 minút dňa 13. novembra. To ukazuje problém so škálovateľnosťou, kde SQL nie je efektívne pri rastúcom objeme dát.

## Najpravdepodobnejšie príčiny a riešenia

### 1. Chýbajúce indexy
S denným nárastom dát musí databáza prehľadávať celé tabuľky (full table scan), čo spomaľuje spracovanie.

**Riešenie:**  
Použite príkaz `EXPLAIN` na analýzu dopytu a následne vytvorte indexy na stĺpcoch používaných v klauzulách `WHERE` a `JOIN`.

### 2. Spracovanie všetkých dát namiesto inkrementálneho spracovania
Transformácia pravdepodobne každý deň spracováva všetky dáta odznova. Doba spracovania 13. novembra (2 h 44 min) nezodpovedá len novým dátam, ale celému historickému objemu.

**Riešenie:**  
Je potrebné upraviť skript na inkrementálne spracovanie. Namiesto spracovania celej histórie načítajte iba dáta, ktoré sa zmenili od posledného behu. Možnosti:  

* **Jednoduchý INSERT:** Pre pridávanie iba nových záznamov (napr. `WHERE created_at > posledny_datum`).  
* **Príkaz MERGE (odporučil by som takmer vždy):** MERGE dokáže v jednej operácii pridať nové záznamy (`INSERT`) a zároveň aktualizovať existujúce, ktoré sa zmenili (`UPDATE`). Je to lepšie metóda, vo všeobecnosti, pre tieto účely.

### 3. Neaktuálne databázové štatistiky
Plánovač databázy môže zvoliť pomalý exekučný plán, pretože jeho štatistiky o objeme a rozložení dát sú staré-chýbajú.

**Riešenie:**  
Pravidelne je dobré aktualizovať databázové štatistiky, napríklad pomocou príkazu `ANALYZE`.
