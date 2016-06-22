package com.hortonworks.ZeroDowntimeDeployment;

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

@SuppressWarnings("deprecation")
public class TestSolr {

	public static void main(String[] args) {

		HttpSolrServer server = new HttpSolrServer(
				"http://localhost:8983/solr/gettingstarted");

		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", "test1");
		doc.addField("accessid", 123);
		try {
			server.add(doc);
			server.commit(false, false);
			System.out
					.println("SolrBolt: successfully added tweet to Solr server");
		} catch (Exception e) {
			e.printStackTrace();
			// collector.fail(input);
		}

	}

}
