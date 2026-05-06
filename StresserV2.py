#!/usr/bin/env python3
"""
Stress Test Tool - Test de charge sur votre propre infrastructure
Utilisation autorisée uniquement sur vos propres serveurs
"""

import asyncio
import aiohttp
import random
import time
import sys
import json
import logging
from datetime import datetime
from typing import List, Dict
from dataclasses import dataclass, asdict
import statistics

# ============================================================
# CONFIGURATION
# ============================================================

@dataclass
class TestConfig:
    target_url: str
    concurrent_requests: int = 100
    duration_seconds: int = 30
    ramp_up_time: int = 5          # Temps pour monter en charge
    request_timeout: int = 10
    follow_redirects: bool = False
    verbose: bool = False
    
    # Patterns de requêtes réalistes
    paths: List[str] = None
    
    def __post_init__(self):
        if self.paths is None:
            self.paths = [
                "/",
                "/api/health",
                "/api/status",
                "/about",
                "/contact",
            ]

# ============================================================
# MOTEUR DE STRESS
# ============================================================

class StressTestEngine:
    """Moteur de test de charge asynchrone haute performance."""
    
    def __init__(self, config: TestConfig):
        self.config = config
        self.stats = {
            "total_requests": 0,
            "successful": 0,
            "failed": 0,
            "timeouts": 0,
            "errors_4xx": 0,
            "errors_5xx": 0,
        }
        self.response_times = []
        self.error_log = []
        self.start_time = None
        
        # Configuration logging
        logging.basicConfig(
            level=logging.INFO if config.verbose else logging.WARNING,
            format='%(asctime)s | %(message)s',
            handlers=[
                logging.FileHandler(f'stress_test_{int(time.time())}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("StressTest")
    
    async def single_request(self, session: aiohttp.ClientSession, 
                            worker_id: int, request_id: int) -> Dict:
        """Effectue une requête unique et mesure les performances."""
        
        url = self.config.target_url.rstrip('/')
        path = random.choice(self.config.paths)
        full_url = f"{url}{path}"
        
        # Headers réalistes
        headers = {
            "User-Agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15",
            ]),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
        }
        
        start_time = time.perf_counter()
        
        try:
            async with session.get(
                full_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.config.request_timeout),
                allow_redirects=self.config.follow_redirects,
                ssl=False
            ) as response:
                elapsed = time.perf_counter() - start_time
                content = await response.read()
                
                result = {
                    "worker_id": worker_id,
                    "request_id": request_id,
                    "url": full_url,
                    "status": response.status,
                    "elapsed": elapsed,
                    "size": len(content),
                    "success": response.status < 400,
                }
                
                # Statistiques
                self.stats["total_requests"] += 1
                if response.status < 400:
                    self.stats["successful"] += 1
                elif 400 <= response.status < 500:
                    self.stats["errors_4xx"] += 1
                else:
                    self.stats["errors_5xx"] += 1
                
                self.response_times.append(elapsed)
                
                if self.config.verbose:
                    self.logger.info(f"  [{worker_id}:{request_id}] {full_url} -> {response.status} ({elapsed:.3f}s)")
                
                return result
                
        except asyncio.TimeoutError:
            self.stats["timeouts"] += 1
            self.stats["failed"] += 1
            elapsed = time.perf_counter() - start_time
            
            self.logger.warning(f"  [{worker_id}:{request_id}] TIMEOUT après {elapsed:.1f}s")
            return {
                "worker_id": worker_id,
                "request_id": request_id,
                "url": full_url,
                "status": 0,
                "elapsed": elapsed,
                "error": "timeout"
            }
            
        except Exception as e:
            self.stats["failed"] += 1
            self.logger.error(f"  [{worker_id}:{request_id}] ERREUR: {e}")
            return {
                "worker_id": worker_id,
                "request_id": request_id,
                "url": full_url,
                "status": 0,
                "elapsed": time.perf_counter() - start_time,
                "error": str(e)
            }
    
    async def worker_loop(self, session: aiohttp.ClientSession, 
                         worker_id: int, stop_event: asyncio.Event):
        """Boucle d'un worker qui envoie des requêtes en continu."""
        request_id = 0
        
        while not stop_event.is_set():
            await self.single_request(session, worker_id, request_id)
            request_id += 1
            
            # Petit délai pour éviter de surcharger la boucle asyncio
            await asyncio.sleep(0.001)
    
    async def ramp_up(self, session: aiohttp.ClientSession, stop_event: asyncio.Event):
        """Monte progressivement en charge."""
        ramp_time = self.config.ramp_up_time
        total_workers = self.config.concurrent_requests
        
        self.logger.info(f"\n📈 Ramp-up: {total_workers} workers en {ramp_time}s")
        
        workers = []
        for i in range(total_workers):
            # Ajoute progressivement les workers
            delay = ramp_time / total_workers
            await asyncio.sleep(delay)
            
            worker = asyncio.create_task(
                self.worker_loop(session, i, stop_event)
            )
            workers.append(worker)
            
            progress = (i + 1) / total_workers * 100
            if (i + 1) % 10 == 0 or i == total_workers - 1:
                self.logger.info(f"  📊 Progress: {progress:.0f}% ({i+1}/{total_workers} workers)")
        
        return workers
    
    async def run(self) -> Dict:
        """Exécute le test de stress complet."""
        
        print(f"""
        ╔══════════════════════════════════════════════════════════╗
        ║              STRESS TEST ENGINE v2.0                    ║
        ╠══════════════════════════════════════════════════════════╣
        ║  Cible: {self.config.target_url:<43}║
        ║  Workers: {self.config.concurrent_requests:<41}║
        ║  Durée: {self.config.duration_seconds}s{' ' * 39}║
        ║  Timeout: {self.config.request_timeout}s{' ' * 37}║
        ╚══════════════════════════════════════════════════════════╝
        """)
        
        self.start_time = time.time()
        stop_event = asyncio.Event()
        
        # Config SSL (désactive les warnings pour les tests)
        connector = aiohttp.TCPConnector(
            ssl=False,
            limit=0,  # Pas de limite de connexions
            limit_per_host=0,
            ttl_dns_cache=300,
            force_close=True
        )
        
        async with aiohttp.ClientSession(
            connector=connector,
            headers={"Connection": "keep-alive"}
        ) as session:
            
            # Phase de ramp-up
            workers = await self.ramp_up(session, stop_event)
            
            # Phase de test soutenu
            self.logger.info(f"\n⚡ Test soutenu: {self.config.duration_seconds}s")
            
            # Affiche les stats en temps réel
            stats_task = asyncio.create_task(self._live_stats())
            
            await asyncio.sleep(self.config.duration_seconds)
            
            # Arrête tout
            stop_event.set()
            for w in workers:
                w.cancel()
            stats_task.cancel()
            
            # Nettoie les tâches restantes
            await asyncio.gather(*workers, return_exceptions=True)
        
        elapsed = time.time() - self.start_time
        return self._generate_report(elapsed)
    
    async def _live_stats(self):
        """Affiche les statistiques en temps réel."""
        try:
            while True:
                await asyncio.sleep(2)
                elapsed = time.time() - self.start_time
                rate = self.stats["total_requests"] / elapsed if elapsed > 0 else 0
                
                print(f"\r  📊 [{elapsed:.0f}s] Req: {self.stats['total_requests']} | "
                      f"✅ {self.stats['successful']} | "
                      f"❌ {self.stats['failed']} | "
                      f"⏱ {self.stats['timeouts']} timeout | "
                      f"📈 {rate:.0f} req/s | "
                      f"4xx: {self.stats['errors_4xx']} | 5xx: {self.stats['errors_5xx']}", 
                      end="", flush=True)
        except asyncio.CancelledError:
            print()
    
    def _generate_report(self, elapsed: float) -> Dict:
        """Génère le rapport final détaillé."""
        
        if not self.response_times:
            return {"error": "Aucune requête effectuée"}
        
        sorted_times = sorted(self.response_times)
        total = self.stats["total_requests"]
        rate = total / elapsed if elapsed > 0 else 0
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "target": self.config.target_url,
            "duration": elapsed,
            "stats": {
                "total_requests": total,
                "successful": self.stats["successful"],
                "failed": self.stats["failed"],
                "timeouts": self.stats["timeouts"],
                "errors_4xx": self.stats["errors_4xx"],
                "errors_5xx": self.stats["errors_5xx"],
            },
            "performance": {
                "requests_per_second": round(rate, 2),
                "avg_response_time": round(statistics.mean(self.response_times), 3) if self.response_times else 0,
                "median_response_time": round(statistics.median(sorted_times), 3) if sorted_times else 0,
                "min_response_time": round(min(self.response_times), 3) if self.response_times else 0,
                "max_response_time": round(max(self.response_times), 3) if self.response_times else 0,
                "p50": round(statistics.median(sorted_times), 3) if sorted_times else 0,
                "p75": round(sorted_times[int(len(sorted_times) * 0.75)], 3) if sorted_times else 0,
                "p90": round(sorted_times[int(len(sorted_times) * 0.90)], 3) if sorted_times else 0,
                "p99": round(sorted_times[int(len(sorted_times) * 0.99)], 3) if sorted_times else 0,
            }
        }
        
        return report


# ============================================================
# RAPPORT FORMATTÉ
# ============================================================

def print_report(report: Dict):
    """Affiche le rapport de manière lisible."""
    
    print("\n" + "=" * 60)
    print("📊 RAPPORT DE TEST DE STRESS")
    print("=" * 60)
    print(f"🕐 {report['timestamp']}")
    print(f"🎯 Cible: {report['target']}")
    print(f"⏱ Durée: {report['duration']:.1f}s")
    print()
    
    s = report['stats']
    print("📈 STATISTIQUES:")
    print(f"   Requêtes totales: {s['total_requests']}")
    print(f"   ✅ Réussies:      {s['successful']} ({s['successful']/s['total_requests']*100:.1f}%)")
    print(f"   ❌ Échouées:      {s['failed']} ({s['failed']/s['total_requests']*100:.1f}%)")
    print(f"   ⏱ Timeouts:      {s['timeouts']}")
    print(f"   4xx:              {s['errors_4xx']}")
    print(f"   5xx:              {s['errors_5xx']}")
    print()
    
    p = report['performance']
    print("⚡ PERFORMANCE:")
    print(f"   📨 Débit:          {p['requests_per_second']} req/s")
    print(f"   ⚡ Temps moyen:    {p['avg_response_time']}s")
    print(f"   📊 Médiane (p50):  {p['p50']}s")
    print(f"   🟡 p75:            {p['p75']}s")
    print(f"   🟠 p90:            {p['p90']}s")
    print(f"   🔴 p99:            {p['p99']}s")
    print(f"   🔵 Minimum:        {p['min_response_time']}s")
    print(f"   🔴 Maximum:        {p['max_response_time']}s")
    print()
    
    # Interprétation
    print("📋 ANALYSE:")
    if p['requests_per_second'] > 1000:
        print("   ✅ Très bonne performance (>1000 req/s)")
    elif p['requests_per_second'] > 500:
        print("   ✅ Bonne performance (500-1000 req/s)")
    elif p['requests_per_second'] > 100:
        print("   ⚠ Performance moyenne (100-500 req/s)")
    else:
        print("   ❌ Performance faible (<100 req/s)")
    
    if s['failed'] / s['total_requests'] > 0.1:
        print("   ❌ Taux d'échec élevé (>10%) - Infrastructure peut-être saturée")
    elif s['failed'] > 0:
        print("   ⚠ Certaines requêtes ont échoué")
    else:
        print("   ✅ Zéro échec - Infrastructure stable")
    
    print("=" * 60)


# ============================================================
# MODE SUPER AGRESSIF (POUR GROSSE INFRA)
# ============================================================

class HeavyStressTest(StressTestEngine):
    """Version agressive pour tester les limites."""
    
    async def worker_loop(self, session, worker_id, stop_event):
        """Boucle sans délai entre les requêtes."""
        request_id = 0
        
        while not stop_event.is_set():
            # Lance 5 requêtes en parallèle par worker
            tasks = [
                self.single_request(session, worker_id, request_id + j)
                for j in range(5)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            request_id += 5


# ============================================================
# ENTRY POINT
# ============================================================

async def main():
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║           STRESS TEST TOOL - Test de charge              ║
    ║      Usage autorisé UNIQUEMENT sur votre infrastructure  ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    # Configuration interactive
    target = input("URL cible (ex: https://votre-site.com) : ").strip()
    
    print("\nConfiguration du test:")
    print("  [1] Léger (50 workers, 10s) - Test rapide")
    print("  [2] Moyen (200 workers, 30s) - Standard")
    print("  [3] Lourd (1000 workers, 60s) - Stress intense")
    print("  [4] Personnalisé")
    
    mode = input("\nChoix (1-4) : ").strip()
    
    configs = {
        "1": TestConfig(target, 50, 10, 2, 5),
        "2": TestConfig(target, 200, 30, 5, 10),
        "3": TestConfig(target, 1000, 60, 10, 15),
    }
    
    if mode in configs:
        config = configs[mode]
    else:
        workers = int(input("Workers simultanés : ") or "100")
        duration = int(input("Durée (secondes) : ") or "30")
        timeout = int(input("Timeout par requête (s) : ") or "10")
        config = TestConfig(target, workers, duration, timeout=timeout)
    
    print(f"\n⚠️  CONFIMATION: Test sur {target}")
    print(f"   Workers: {config.concurrent_requests}")
    print(f"   Durée: {config.duration_seconds}s")
    print(f"   Cela générera un trafic important vers votre serveur.")
    
    confirm = input("\nTapez 'STRESS' pour confirmer : ")
    if confirm != "STRESS":
        print("Test annulé.")
        return
    
    # Lancement
    engine = StressTestEngine(config)
    report = await engine.run()
    
    # Rapport
    print_report(report)
    
    # Sauvegarde JSON
    filename = f"stress_report_{int(time.time())}.json"
    with open(filename, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\n📁 Rapport sauvegardé: {filename}")


if __name__ == "__main__":
    asyncio.run(main())
