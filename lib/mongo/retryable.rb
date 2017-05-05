# Copyright (C) 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo

  # Defines basic behaviour around retrying operations.
  #
  # @since 2.1.0
  module Retryable

    NOT_MASTER = 'not master'.freeze
    NOT_CONTACT_PRIMARY = 'could not contact primary'.freeze

    # Execute a read operation with a retry.
    #
    # @api private
    #
    # @example Execute the read.
    #   read_with_retry do
    #     ...
    #   end
    #
    # @note This only retries read operations on socket errors.
    #
    # @param [ Integer ] attempt The retry attempt count - for internal use.
    # @param [ Proc ] block The block to execute.
    #
    # @return [ Result ] The result of the operation.
    #
    # @since 2.1.0
    def read_with_retry
      attempt = 0
      begin
        attempt += 1
        yield
      rescue Error::SocketError, Error::SocketTimeoutError, Error::OperationFailure, Mongo::Auth::Unauthorized => e
        connection_error = e.kind_of?(Error::SocketError) || e.kind_of?(Error::SocketTimeoutError)
        operation_failure = e.kind_of?(Error::OperationFailure)
        auth_error = e.kind_of?(Mongo::Auth::Unauthorized)
        sleep_multiplier = 1

        if connection_error || auth_error || (operation_failure && (e.unauthorized? || e.retryable?))
          cluster.scan!
        end

        if (operation_failure && e.unauthorized?) || auth_error
          sleep_multiplier = 2
        end

        if operation_failure && cluster.sharded? && e.retryable?
          Mongo::Logger.logger.warn("[jontest] got error for read on #{cluster.servers.inspect}: #{e.inspect}, attempt #{attempt}, max retries is #{cluster.max_read_retries}")
        else
          Mongo::Logger.logger.warn("[jontest] got error for read on #{cluster.servers.inspect}: #{e.inspect}, attempt #{attempt}, max retries is #{cluster.max_read_retries}")
        end
        if connection_error || (operation_failure && cluster.sharded? && (e.retryable? || e.unauthorized?)) || auth_error
          if attempt < cluster.max_read_retries
            if (operation_failure && e.unauthorized?) || auth_error
              Mongo::Logger.logger.warn("[jontest] got unauthorized for read on #{cluster.servers.inspect}, re-authenticating, attempt is #{attempt}, #{e.inspect()}")
              begin
                cluster.servers.each {|server| server.context.with_connection {|conn| conn.authenticate!(server.options) } }
              rescue Mongo::Auth::Unauthorized
                # Disconnect before we retry again
                sleep_multiplier = 2
                cluster.disconnect!
              end
            end

            # We don't scan the cluster in this case as Mongos always returns
            # ready after a ping no matter what the state behind it is.
            sleep(cluster.read_retry_interval * sleep_multiplier)
            retry
          else
            raise e
          end
        else
          raise e
        end
      end
    end

    # Execute a read operation with a single retry.
    #
    # @api private
    #
    # @example Execute the read.
    #   read_with_one_retry do
    #     ...
    #   end
    #
    # @note This only retries read operations on socket errors.
    #
    # @param [ Proc ] block The block to execute.
    #
    # @return [ Result ] The result of the operation.
    #
    # @since 2.2.6
    def read_with_one_retry
      yield
    rescue Error::SocketError, Error::SocketTimeoutError
      yield
    end

    # Execute a write operation with a retry.
    #
    # @api private
    #
    # @example Execute the write.
    #   write_with_retry do
    #     ...
    #   end
    #
    # @note This only retries operations on not master failures, since it is
    #   the only case we can be sure a partial write did not already occur.
    #
    # @param [ Proc ] block The block to execute.
    #
    # @return [ Result ] The result of the operation.
    #
    # @since 2.1.0

    def write_with_retry
      attempt = 0
      begin
        attempt += 1
        yield
      rescue Error::SocketError, Error::SocketTimeoutError, Error::OperationFailure, Error::NoServerAvailable, Mongo::Auth::Unauthorized => e
        connection_error = e.kind_of?(Error::SocketError) || e.kind_of?(Error::SocketTimeoutError)
        operation_failure = e.kind_of?(Error::OperationFailure)
        no_server_available = e.kind_of?(Error::NoServerAvailable)
        auth_error = e.kind_of?(Mongo::Auth::Unauthorized)
        not_master = e.message.include?(NOT_MASTER) || e.message.include?(NOT_CONTACT_PRIMARY)
        batch_write = e.message.include?('no progress was made executing batch write op'.freeze)
        write_unavailable = e.message.include?('write results unavailable'.freeze)
        sleep_multiplier = 1
        if connection_error || not_master || batch_write || no_server_available || auth_error || write_unavailable
          if connection_error
            Mongo::Logger.logger.warn("[jontest] got connection error in write on #{cluster.servers.inspect}, attempt #{attempt}")
          elsif not_master
            Mongo::Logger.logger.warn("[jontest] got not master in write on #{cluster.servers.inspect}, attempt #{attempt}")
          elsif batch_write
            Mongo::Logger.logger.warn("[jontest] got batch write failure in write on #{cluster.servers.inspect}, attempt #{attempt}")
          elsif write_unavailable
            Mongo::Logger.logger.warn("[jontest] got write unavailable in write on #{cluster.servers.inspect}, attempt #{attempt}")
          elsif no_server_available
            Mongo::Logger.logger.warn("[jontest] got no server available in write on #{cluster.servers.inspect}, will retry one more time")
            attempt = cluster.max_read_retries - 1
          end
          cluster.scan!
        end

        if (operation_failure && e.unauthorized?) || auth_error
          sleep_multiplier = 2
        end

        if connection_error || (operation_failure && (e.retryable? || e.unauthorized?)) || no_server_available || auth_error || write_unavailable
          # We're using max_read_retries here but if we got one of the errors that is causing us to be here, we should be retrying
          # often anyway
          if attempt < cluster.max_read_retries
            if (operation_failure && e.unauthorized?) || auth_error
              Mongo::Logger.logger.warn("[jontest] got unauthorized for write on #{cluster.servers.inspect}, re-authenticating, attempt is #{attempt}, #{e.inspect()}")
              begin
                cluster.servers.each {|server| server.context.with_connection {|conn| conn.authenticate!(server.options) } }
              rescue Mongo::Auth::Unauthorized
                sleep_multiplier = 2
                # Disconnect before we retry again
                cluster.disconnect!
              end
            end

            # We don't scan the cluster in this case as Mongos always returns
            # ready after a ping no matter what the state behind it is.
            sleep(cluster.read_retry_interval * sleep_multiplier)
            retry
          else
            raise e
          end
        else
          raise e
        end
      end
    end
  end
end
