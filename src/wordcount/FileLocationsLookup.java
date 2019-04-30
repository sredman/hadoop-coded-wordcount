package wordcount;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class FileLocationsLookup {
	final long blockSize;
	private final List<BlockInfo> blockLocations;
	
	public FileLocationsLookup(String filename) throws SAXException, IOException, ParserConfigurationException, FileLocationsException {
		InputStream xmlFile = getClass().getResourceAsStream(filename);

		Document xmlDocument = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);

		NodeList documentRoot = xmlDocument.getElementsByTagName("BlockLocationInfo");

		blockSize = Integer.parseInt(((Element) documentRoot.item(0)).getAttribute("blockSize"));

		NodeList blockInfoNodes = xmlDocument.getElementsByTagName("blocks").item(0).getChildNodes();

		blockLocations = new ArrayList<>(blockInfoNodes.getLength());
		int offsetCounter = 0;
		for (int index = 0; index < blockInfoNodes.getLength(); index++) {
			Node block = blockInfoNodes.item(index);
			if (block.getNodeType() != Node.ELEMENT_NODE) {
				continue;
			}
			Element eBlock = (Element) block;

			String blockName = eBlock.getNodeName();
			long blockSize = Long.parseLong(eBlock.getAttributes().getNamedItem("size").getNodeValue());
			assert blockSize <= this.blockSize : "Block " + blockName + " has size larger than the configured block size";

			NodeList locations = eBlock.getElementsByTagName("location");
			List<String> locationsInfo = new ArrayList<>(locations.getLength());

			for (int locationIndex = 0; locationIndex < locations.getLength(); locationIndex++) {
				Element location = (Element) locations.item(locationIndex);
				locationsInfo.add(location.getTextContent());
			}
			blockLocations.add(new BlockInfo(
					blockName,
					offsetCounter,
					blockSize,
					locationsInfo
					));
			offsetCounter += blockSize;
		}
		return;
	}

	public BlockInfo getBlockContainingOffset(long offset) {
		int blockNumber = (int)(offset / (blockSize*1024));
		return blockLocations.get(blockNumber);
	}

	public class BlockInfo {
		final String name;
		final long offset;
		final long size;
		final List<String> locations;

		public BlockInfo(String name, long offset, long size, List<String> locations) {
			this.name = name;
			this.offset = offset;
			this.size = size;
			this.locations = locations;
		}
	}

	public static class FileLocationsException extends Exception {

		private static final long serialVersionUID = 3435995174783310221L;

		public FileLocationsException(String message) {
			super(message);
		}
	}
}
