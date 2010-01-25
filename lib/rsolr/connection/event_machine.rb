raise RuntimeError, "EventMachine connection requires Ruby 1.9" if RUBY_VERSION < '1.9'

require 'em-http'
require 'fiber'

#
# Evented Connection for standard HTTP Solr server
#
class RSolr::Connection::EventMachine
  
  include RSolr::Connection::Utils
  
  attr_reader :opts, :uri

  REQUEST_CLASS = EM::HttpRequest
  
  # opts can have:
  #   :url => 'http://localhost:8080/solr'
  def initialize opts={}
    opts[:url] ||= 'http://127.0.0.1:8983/solr'
    @opts = opts
    @uri = URI.parse opts[:url]
  end
  
  # send a request to the connection
  # request '/update', :wt=>:xml, '</commit>'
  def request path, params={}, *extra
    opts = extra[-1].kind_of?(Hash) ? extra.pop : {}
    data = extra[0]
    # force a POST, use the query string as the POST body
    if opts[:method] == :post and data.to_s.empty?
      http_context = self.post(path, hash_to_query(params), {}, {'Content-Type' => 'application/x-www-form-urlencoded'})
    else
      if data
        # standard POST, using "data" as the POST body
        http_context = self.post(path, data, params, {"Content-Type" => 'text/xml; charset=utf-8'})
      else
        # standard GET
        http_context = self.get(path, params)
      end
    end
    raise RSolr::RequestError, "Solr Response: #{http_context[:message]}" unless http_context[:status_code] == 200
    http_context
  end
  
  protected
  
  def connection path
    REQUEST_CLASS.new("#{@uri.to_s}#{path}")
  end
  
  def get path, params={}
    # this yield/resume business is complicated by em-http's mocking support which
    # yields to the callback immediately rather than from another fiber.
    yielding = true
    fiber = Fiber.current
    http_response = self.connection(path).get :query => params, :timeout => 5
    http_response.callback do
      yielding = false
      fiber.resume if Fiber.current != fiber
    end
    Fiber.yield if yielding
    create_http_context http_response, path, params
  end
  
  def post path, data, params={}, headers={}
    yielding = true
    fiber = Fiber.current
    http_response = self.connection(path).post :query => params, :body => data, :head => headers, :timeout => 5
    http_response.callback do
      yielding = false
      fiber.resume if Fiber.current != fiber
    end
    Fiber.yield if yielding
    create_http_context http_response, path, params, data, headers
  end
  
  def create_http_context http_response, path, params, data=nil, headers={}
    full_url = "#{@uri.to_s}#{path}"
    {
      :status_code=>http_response.response_header.status,
      :url=>full_url,
      :body=>encode_utf8(http_response.response),
      :path=>path,
      :params=>params,
      :data=>data,
      :headers=>headers,
    }
  end
  
  # encodes the string as utf-8 in Ruby 1.9
  # returns the unaltered string in Ruby 1.8
  def encode_utf8 string
    (string.respond_to?(:force_encoding) and string.respond_to?(:encoding)) ?
      string.force_encoding(Encoding::UTF_8) : string
  end
  
  # accepts a path/string and optional hash of query params
  def build_url path, params={}
    full_path = @uri.path + path
    super full_path, params, @uri.query
  end
  
end