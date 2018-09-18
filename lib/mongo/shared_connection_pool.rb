module Mongo
  class SharedConnectionPool
    MUTEX = Mutex.new
    @pools = {}

    # Get the shared connection pool for the server.
    #
    # @param [ Server ] server The server.
    #
    # @return [ Server::ConnectionPool ] The connection pool.
    def self.get(server)
      MUTEX.synchronize do
        @pools[server.address] ||= Server::ConnectionPool.get(server)
      end
    end
  end
end
