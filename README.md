# findFullData
## Identifying Blacklisted Blockchain Nodes and Their blacklisted Neighbors' Transaction Info

To achieve the task of finding blockchain nodes on blacklists and retrieving transaction information for their blacklisted neighbors up to the fourth degree

1. **Identify Blacklisted Nodes**: Locate nodes that are listed on blacklists within the blockchain network.

2. **Find Blacklisted Neighbors of Blacklisted Nodes**: For each blacklisted node, retrieve the list of its Blacklisted neighboring nodes (direct connections without directly ring neighbors).

3. **Obtain Transaction Information**: For each neighboring node, retrieve transaction information associated with them. This could include transaction history, sender, recipient, timestamp, and transaction value.

4. **Aggregate and Present Information**: Organize the collected data, including blacklisted nodes, their blacklisted neighbors, and transaction information, up to the fourth degree, into a structured format for analysis or reporting.
