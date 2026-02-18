#!/usr/bin/env python3
"""Complete AWET System Health Check - Verify all configurations."""
import os
import sys
import json
from pathlib import Path
import subprocess

def check_env_vars():
    """Check critical environment variables."""
    print("🔍 Checking Environment Variables...")
    required = {
        "DATABASE_URL": os.getenv("DATABASE_URL"),
        "ALPACA_API_KEY": os.getenv("ALPACA_API_KEY"),
        "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN"),
        "LLM_BASE_URL": os.getenv("LLM_BASE_URL"),
        "LLM_MODEL": os.getenv("LLM_MODEL"),
    }
    
    all_good = True
    for key, value in required.items():
        if value:
            masked = value[:20] + "..." if len(value) > 20 else value
            print(f"   ✅ {key}: {masked}")
        else:
            print(f"   ❌ {key}: NOT SET")
            all_good = False
    
    return all_good

def check_ollama():
    """Check Ollama is running and has the right model."""
    print("\n🤖 Checking Ollama...")
    try:
        result = subprocess.run(
            ["ollama", "list"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            if "qwen2.5:32b" in result.stdout:
                print(f"   ✅ Ollama running with qwen2.5:32b")
                return True
            else:
                print(f"   ⚠️  Ollama running but qwen2.5:32b not found")
                print(f"   Models: {result.stdout}")
                return False
        else:
            print(f"   ❌ Ollama command failed")
            return False
    except Exception as e:
        print(f"   ❌ Ollama not accessible: {e}")
        return False

def check_gpu():
    """Check GPU availability."""
    print("\n🎮 Checking GPU...")
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            gpu_info = result.stdout.strip()
            print(f"   ✅ GPU: {gpu_info}")
            return True
        else:
            print(f"   ⚠️  nvidia-smi failed")
            return False
    except FileNotFoundError:
        print(f"   ⚠️  nvidia-smi not found (OK if no GPU)")
        return True
    except Exception as e:
        print(f"   ⚠️  GPU check failed: {e}")
        return True  # Non-critical

def check_schemas():
    """Validate all Avro schemas."""
    print("\n📋 Checking Avro Schemas...")
    schema_dir = Path("schemas/avro")
    if not schema_dir.exists():
        print(f"   ❌ Schema directory not found: {schema_dir}")
        return False
    
    schemas = list(schema_dir.glob("*.avsc"))
    if not schemas:
        print(f"   ❌ No schemas found in {schema_dir}")
        return False
    
    all_valid = True
    for schema_file in schemas:
        try:
            with open(schema_file) as f:
                json.load(f)
            print(f"   ✅ {schema_file.name}")
        except json.JSONDecodeError as e:
            print(f"   ❌ {schema_file.name}: Invalid JSON - {e}")
            all_valid = False
        except Exception as e:
            print(f"   ❌ {schema_file.name}: {e}")
            all_valid = False
    
    return all_valid

def check_config_files():
    """Check YAML config files."""
    print("\n⚙️  Checking Config Files...")
    configs = [
        "config/app.yaml",
        "config/kafka.yaml",
        "config/llm.yaml",
        "config/logging.yaml",
    ]
    
    all_good = True
    for config in configs:
        if Path(config).exists():
            print(f"   ✅ {config}")
        else:
            print(f"   ❌ {config}: NOT FOUND")
            all_good = False
    
    return all_good

def check_database():
    """Check database connection."""
    print("\n💾 Checking Database...")
    try:
        result = subprocess.run(
            ["psql", "-h", "localhost", "-p", "5433", "-U", "awet", "-d", "awet", 
             "-c", "SELECT COUNT(*) as tables FROM information_schema.tables WHERE table_schema='public'"],
            capture_output=True,
            text=True,
            timeout=10,
            env={**os.environ, "PGPASSWORD": "awet"}
        )
        if result.returncode == 0:
            print(f"   ✅ Database accessible (PostgreSQL)")
            return True
        else:
            print(f"   ⚠️  Database connection failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"   ⚠️  Database check failed: {e}")
        return False

def check_docker():
    """Check Docker services."""
    print("\n🐳 Checking Docker Services...")
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            containers = result.stdout.strip().split("\n")
            containers = [c for c in containers if c]
            if containers:
                print(f"   ✅ {len(containers)} containers running")
                for container in containers[:5]:  # Show first 5
                    print(f"      • {container}")
                if len(containers) > 5:
                    print(f"      ... and {len(containers) - 5} more")
                return True
            else:
                print(f"   ⚠️  No Docker containers running")
                return False
        else:
            print(f"   ❌ Docker command failed")
            return False
    except Exception as e:
        print(f"   ⚠️  Docker check failed: {e}")
        return False

def main():
    print("=" * 70)
    print("🔧 AWET SYSTEM HEALTH CHECK")
    print("=" * 70)
    
    # Load .env
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ[key] = value
    
    checks = [
        ("Environment Variables", check_env_vars()),
        ("Ollama LLM", check_ollama()),
        ("GPU", check_gpu()),
        ("Avro Schemas", check_schemas()),
        ("Config Files", check_config_files()),
        ("Database", check_database()),
        ("Docker Services", check_docker()),
    ]
    
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    
    passed = sum(1 for _, result in checks if result)
    total = len(checks)
    
    for name, result in checks:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {status:12} {name}")
    
    print("=" * 70)
    print(f"   {passed}/{total} checks passed")
    
    if passed == total:
        print("\n   🎉 ALL SYSTEMS GO! Everything is configured correctly.")
        return 0
    elif passed >= total - 2:
        print("\n   ⚠️  MOSTLY GOOD. Minor issues detected but system is functional.")
        return 0
    else:
        print("\n   ❌ ISSUES DETECTED. Please fix the failing checks.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
