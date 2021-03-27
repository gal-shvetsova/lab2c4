package org.example;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.StreamReaderDelegate;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class OsmXMLProcessor implements AutoCloseable {
    private static final XMLInputFactory xmlFactory = XMLInputFactory.newFactory();

    private final XMLStreamReader reader;

    public OsmXMLProcessor(InputStream xmlStream) throws XMLStreamException {
        this.reader = new OsmXMLDecorator(xmlFactory.createXMLStreamReader(xmlStream));
    }

    public List<Node> unmarshalNextNodes(int n) throws XMLStreamException, JAXBException {
        JAXBContext context = JAXBContext.newInstance(Node.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();

        List<Node> result = new ArrayList<>();
        while (this.reader.hasNext()) {
            int event = this.reader.next();
            if (event == XMLStreamConstants.START_ELEMENT && getTagName().equals("node")) {
                Node node = (Node) unmarshaller.unmarshal(this.reader);
                result.add(node);
                if (result.size() == n) {
                    return result;
                }
            }
        }
        return result;
    }

    private String getTagName() {
        return this.reader.getLocalName();
    }

    @Override
    public void close() throws Exception {
        this.reader.close();
    }

    private static class OsmXMLDecorator extends StreamReaderDelegate {
        private static final String SCHEMA_INSTANCE_URL = "http://openstreetmap.org/osm/0.6";

        public OsmXMLDecorator(XMLStreamReader reader) {
            super(reader);
        }

        @Override
        public String getNamespaceURI() {
            return SCHEMA_INSTANCE_URL;
        }
    }
}
