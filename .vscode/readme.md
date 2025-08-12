# Dokumentace VS Code nastavení projektu

## Účel souboru `.vscode/settings.json`

Tento soubor obsahuje nastavení specifická pro workspace (pracovní prostor) VS Code editoru pro projekt `blitz_agent`. Nastavení se aplikují pouze při práci s tímto projektem a přepisují globální nastavení uživatele.

## Popis jednotlivých parametrů

### Automatické úpravy souborů
```json
"files.trimTrailingWhitespace": true
```
**Účel**: Automaticky odstraní mezery na koncích řádků při uložení souboru
**Důvod**: Eliminuje pylint chyby typu `C0303: Trailing whitespace`

```json
"files.insertFinalNewline": true
```
**Účel**: Automaticky přidá prázdný řádek na konec souboru při uložení
**Důvod**: Opravuje pylint chyby `C0304: Final newline missing`

```json
"files.trimFinalNewlines": true
```
**Účel**: Odstraní nadbytečné prázdné řádky na konci souboru
**Důvod**: Opravuje pylint chyby `C0305: Trailing newlines`

### Vizuální zobrazení
```json
"editor.renderWhitespace": "trailing"
```
**Účel**: Zobrazuje pouze mezery na koncích řádků jako viditelné tečky
**Výhoda**: Umožňuje vizuálně identifikovat problematické mezery před uložením

### Python linting (kontrola kódu)
```json
"python.linting.enabled": true
```
**Účel**: Aktivuje kontrolu Python kódu v reálném čase
**Výsledek**: Zobrazuje chyby a varování přímo v editoru

```json
"python.linting.pylintEnabled": true
```
**Účel**: Používá pylint jako nástroj pro kontrolu kódu
**Důvod**: Pylint je již nakonfigurován v projektu pomocí `.pylintrc`

```json
"python.linting.pylintPath": "./venv/bin/pylint"
```
**Účel**: Specifikuje cestu k pylint nástroju z virtuálního prostředí projektu
**Výhoda**: Používá stejnou verzi pylint jako má projekt, ne globální instalaci

## Výsledný efekt

Po aplikování těchto nastavení bude VS Code:
- ✅ Automaticky čistit kód při každém uložení
- ✅ Zobrazovat chyby kódu v reálném čase
- ✅ Používat projektové Python prostředí
- ✅ Minimalizovat pylint varování o formátování
- ✅ Udržovat konzistentní styl kódu v celém projektu

## Spuštění
Nastavení se aktivuje automaticky při otevření projektu ve VS Code. Není potřeba žádná další konfigurace.





## Instalace a nastavení pylint ve VS Code

### 1. Instalace pylint
Pylint musí být nainstalován v Python prostředí projektu:

```bash
# Aktivace virtuálního prostředí
source venv/bin/activate

# Instalace pylint
pip install pylint

# Ověření instalace
pylint --version
```

### 2. Instalace Python rozšíření pro VS Code
1. Otevřete VS Code
2. Stiskněte `Cmd+Shift+X` (Extensions)
3. Vyhledejte "Python" od Microsoft
4. Klikněte na "Install"

### 3. Konfigurace interpreteru
1. Otevřete projekt ve VS Code
2. Stiskněte `Cmd+Shift+P` → "Python: Select Interpreter"
3. Vyberte interpreter z vašeho `venv`: `./venv/bin/python`

### 4. Ověření nastavení linteru
1. Otevřete Python soubor (např. `app.py`)
2. Stiskněte `Cmd+Shift+P` → "Python: Select Linter"
3. Vyberte "pylint"
4. VS Code by měl automaticky najít pylint v `./venv/bin/pylint`

### 5. Ruční spuštění pylint
```bash
# Z terminálu (s aktivním venv)
pylint app.py

# Nebo přímo s cestou
./venv/bin/pylint app.py

# Kontrola celého src adresáře
./venv/bin/pylint src/
```

### 6. Řešení běžných problémů

**Problem**: VS Code nevidí pylint
```bash
# Řešení: Ověřte cestu a reinstalujte
which pylint
pip uninstall pylint
pip install pylint
```

**Problem**: "Module not found" chyby
- Ujistěte se, že VS Code používá správný Python interpreter z venv
- Restartujte VS Code po změně interpreteru

**Problem**: Pylint nerespektuje .pylintrc
- Ověřte, že `.pylintrc` je v root adresáři projektu
- Použijte `pylint --generate-rcfile` pro vytvoření nového konfiguračního souboru

### 7. Užitečné klávesové zkratky
- `Cmd+Shift+M` - Otevřít panel "Problems" s chybami
- `F8` - Přejít na další chybu/varování
- `Shift+F8` - Přejít na předchozí chybu/varování
- `Cmd+.` - Zobrazit rychlé opravy (Quick Fix)

### 8. Dodatečná nastavení (volitelné)
Pro ještě lepší zážitek můžete přidat do `settings.json`:

```json
{
    "python.linting.lintOnSave": true,
    "python.linting.maxNumberOfProblems": 100,
    "python.linting.pylintArgs": [
        "--load-plugins=pylint_django",
        "--errors-only"
    ]
}
```

**Vysvětlení**:
- `lintOnSave`: Spustí pylint při každém uložení
- `maxNumberOfProblems`: Maximální počet zobrazených problémů
- `pylintArgs`: Dodatečné argumenty pro pylint (např. pro Django projekty)
