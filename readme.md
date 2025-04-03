Business data is often stored in relational databases like MySQL, while cached data resides in Redis. However, when dealing with cross-database transactions - such as deducting funds from a user's account in MySQL while simultaneously updating their cached balance in Redis - ensuring both operations either succeed together or fail together becomes a critical challenge.


This demo project is a simplified implementation to explore potential solutions for such consistency challenges (not production-ready).
