require 'em-http'
require 'em-http/mock'

describe RSolr::Connection::EventMachine do
  
  RSolr::Connection::EventMachine::REQUEST_CLASS = EM::MockHttpRequest
  
  def new_connection
    RSolr::Connection::EventMachine.new
  end
  
  context '#request' do
    it 'should forward simple, non-data calls to #get' do
      
      EM.run do
        EM.add_timer(1) do
          EM.stop
        end
        
        EM::MockHttpRequest.pass_through_requests = false
        body = <<-EOM
HTTP/1.1 200 OK
Date: Mon, 16 Nov 2009 20:39:15 GMT
Expires: -1
Cache-Control: private, max-age=0
Content-Type: text/xml; charset=utf-8
Connection: close

<?xml version="1.0" encoding="UTF-8"?>
<response>
<lst name="responseHeader"><int name="status">0</int><int name="QTime">1</int><lst name="params"><str name="q">a</str></lst></lst><result name="response" numFound="0" start="0"/>
</response>
EOM
        EM::MockHttpRequest.register 'http://127.0.0.1:8983/solr/select?q=a', :get, body

        Fiber.new do
          begin
            http = new_connection
            resp = http.request('/select', :q=>'a')
            resp[:status_code].should == 200
          rescue Exception => ex
            puts ex.message
            puts ex.backtrace.join("\n")
          end
          EM.stop
        end.resume
      end
      
    end
  end
end