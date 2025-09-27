# Riešenie úlohy 2: Analytická úloha

Riešenie analytickej časti prípadovej štúdie. Dáta sú spracované skriptom **etl_analysis.py** a vizualizované v interaktívnom dashborde pomocou **visualization.py**.

---

### 2.1 Doplnenie názvu mesta k objednávke 

* **Riešenie**: Názvy miest boli k objednávkam doplnené napojením na verejné datasety PSČ pre SK, CZ a HU.
* **Výstup**: Rebríček TOP 20 miest podľa priemernej hodnoty objednávky (AOV) a interaktívna mapa s počtom objednávok.

---

### 2.2 Nová kamenná predajňa 

* **Riešenie**: Lokalita bola navrhnutá na základe miest s najvyššími tržbami, ktoré sú zároveň najviac vzialené existujúcich predajní (Košice, Budapešť, Praha).
* **Dôvod**: Týmto sa cieli na nový trh a predchádza sa kanibalizácii predajov.

---

### 2.3 Priemerná mesačná marža produktu 

* **Riešenie**: Marža bola vypočítaná ako `(predajná cena - nákupná cena) * množstvo` a agregovaná na mesačnej báze pre každý produkt.
* **Výstup**: Interaktívny graf vývoja marže s možnosťou filtrovania produktov.

---

### 2.4 Najpredávanejšie dvojice produktov 

* **Riešenie**: Dvojice boli identifikované zoskupením položiek podľa objednávok a spočítaním frekvencie jednotlivých kombinácií.

---

### Inštrukcie na spustenie 

1.  **Inštalácia závislostí**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Spracovanie dát**: (Uistite sa, že vstupné dáta sú v priečinku `data/in/`)
    ```bash
    python src/etl_analysis.py
    ```

3.  **Vizualizácia**:
    ```bash
    streamlit run src/bi_visualization.py
    ```

3. **Vizualizácia - Online Dashboard**:
    Interaktívny dashboard je dostupný online na Streamlit Cloud:
    https://gymbeam-bi-vis.streamlit.app/