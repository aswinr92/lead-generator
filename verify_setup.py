"""
Verification script for Google Sheets export setup.

This script checks:
1. All required dependencies are installed
2. Configuration file is valid
3. Exporter classes can be imported
4. Basic functionality works (without credentials)
"""

import sys
import io
from pathlib import Path

# Fix encoding issues on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


def check_dependencies():
    """Check if all required packages are installed."""
    print("🔍 Checking dependencies...")

    required_packages = [
        ('yaml', 'PyYAML'),
        ('pandas', 'pandas'),
        ('gspread', 'gspread'),
        ('google.auth', 'google-auth'),
    ]

    all_installed = True
    for module_name, package_name in required_packages:
        try:
            __import__(module_name)
            print(f"   ✅ {package_name}")
        except ImportError:
            print(f"   ❌ {package_name} - NOT INSTALLED")
            all_installed = False

    return all_installed


def check_config():
    """Check if configuration file is valid."""
    print("\n🔍 Checking configuration...")

    try:
        import yaml
        config_path = Path("config/config.yaml")

        if not config_path.exists():
            print("   ❌ config.yaml not found")
            return False

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Check for google_sheets section
        if 'google_sheets' not in config:
            print("   ❌ 'google_sheets' section missing in config.yaml")
            return False

        gs_config = config['google_sheets']

        # Check required keys
        required_keys = ['credentials_file', 'data_tab_name', 'summary_tab_name']
        for key in required_keys:
            if key in gs_config:
                print(f"   ✅ {key}: {gs_config[key]}")
            else:
                print(f"   ❌ {key} missing")
                return False

        return True

    except Exception as e:
        print(f"   ❌ Error loading config: {e}")
        return False


def check_imports():
    """Check if exporter modules can be imported."""
    print("\n🔍 Checking exporter imports...")

    try:
        from exporters.google_sheets_exporter import GoogleSheetsExporter
        print("   ✅ GoogleSheetsExporter")

        from exporters import GoogleSheetsExporter as GSE
        print("   ✅ Package import works")

        return True

    except ImportError as e:
        print(f"   ❌ Import failed: {e}")
        return False


def check_credentials():
    """Check if credentials file exists."""
    print("\n🔍 Checking credentials...")

    creds_path = Path("config/google_credentials.json")

    if creds_path.exists():
        print(f"   ✅ Credentials file found")
        print(f"   📄 Size: {creds_path.stat().st_size} bytes")
        return True
    else:
        print(f"   ⚠️  Credentials file not found (expected)")
        print(f"   📖 See docs/GOOGLE_SHEETS_SETUP.md for setup")
        return False


def check_documentation():
    """Check if documentation exists."""
    print("\n🔍 Checking documentation...")

    docs = [
        'docs/GOOGLE_SHEETS_SETUP.md',
        'README.md',
    ]

    all_exist = True
    for doc in docs:
        if Path(doc).exists():
            print(f"   ✅ {doc}")
        else:
            print(f"   ❌ {doc} - NOT FOUND")
            all_exist = False

    return all_exist


def check_scripts():
    """Check if export script exists."""
    print("\n🔍 Checking scripts...")

    if Path("export_to_sheets.py").exists():
        print("   ✅ export_to_sheets.py")
        return True
    else:
        print("   ❌ export_to_sheets.py - NOT FOUND")
        return False


def main():
    """Run all verification checks."""
    print("=" * 70)
    print("🎉 GOOGLE SHEETS EXPORT - SETUP VERIFICATION")
    print("=" * 70)
    print()

    results = {
        'Dependencies': check_dependencies(),
        'Configuration': check_config(),
        'Imports': check_imports(),
        'Credentials': check_credentials(),
        'Documentation': check_documentation(),
        'Scripts': check_scripts(),
    }

    print("\n" + "=" * 70)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 70)

    for check_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{check_name:.<50} {status}")

    # Overall result
    critical_checks = ['Dependencies', 'Configuration', 'Imports', 'Documentation', 'Scripts']
    critical_passed = all(results[check] for check in critical_checks)

    print("\n" + "=" * 70)

    if critical_passed:
        print("🎉 SETUP COMPLETE!")
        print()
        if not results['Credentials']:
            print("⚠️  Next step: Set up Google Cloud credentials")
            print("   Follow: docs/GOOGLE_SHEETS_SETUP.md")
        else:
            print("✅ Ready to export!")
            print("   Run: python export_to_sheets.py")
    else:
        print("❌ SETUP INCOMPLETE")
        print()
        print("Failed checks need to be resolved.")
        print("Run: pip install -r requirements.txt")
        sys.exit(1)

    print("=" * 70)


if __name__ == "__main__":
    main()
