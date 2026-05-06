#!/usr/bin/env python3
"""
TEST DE CHARGE LÉGITIME - Uniquement sur vos propres serveurs
Utilisez des outils comme Locust, Apache Bench, ou siege
"""

# Exemple de test de charge avec asyncio (VOS SERVEURS UNIQUEMENT)
import asyncio
import aiohttp
import time

async def test_de_charge_legitime():
    """
    Test de charge sur VOTRE PROPRE serveur UNIQUEMENT.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(100):  # 100 requêtes simultanées
            tasks.append(session.get("http://VOTRE_SERVEUR:8080/test"))
        
        start = time.time()
        responses = await asyncio.gather(*tasks)
        elapsed = time.time() - start
        
        print(f"Test de charge terminé: {len(responses)} requêtes en {elapsed:.2f}s")
        print(f"Rythme: {len(responses)/elapsed:.0f} req/s")
        
        for r in responses[:5]:
            print(f"  Status: {r.status}")

# ============================================================
# OUTILS LÉGAUX RECOMMANDÉS
# ============================================================

OUTILS_LEGAUX = """
=== OUTILS DE TEST DE CHARGE LÉGAUX ===

1. Apache Bench (ab) - Test simple
   $ ab -n 1000 -c 50 http://votre-serveur/

2. Siege - Test réaliste
   $ siege -c 100 -t 30s http://votre-serveur/

3. Locust - Tests distribués (Python)
   $ pip install locust
   $ locust -f locustfile.py

4. wrk - Haute performance
   $ wrk -t 12 -c 400 -d 30s http://votre-serveur/

5. hping3 - Test de robustesse réseau
   $ hping3 -S -p 80 --flood VOTRE_SERVEUR
   (⚠ UNIQUEMENT SUR VOTRE PROPRE INFRASTRUCTURE)
"""

print(OUTILS_LEGAUX)
