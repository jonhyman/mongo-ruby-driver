require 'spec_helper'

describe Mongo::Retryable do

  let(:klass) do
    Class.new do
      include Mongo::Retryable

      attr_reader :cluster
      attr_reader :operation

      def initialize(operation, cluster)
        @operation = operation
        @cluster = cluster
      end

      def max_read_retries
        cluster.max_read_retries
      end

      def read_retry_interval
        cluster.read_retry_interval
      end

      def read
        read_with_retry do
          operation.execute
        end
      end

      def write
        write_with_retry(nil, nil) do
          operation.execute
        end
      end
    end
  end

  let(:operation) do
    double('operation')
  end

  let(:cluster) do
    double('cluster', next_primary: server_selector)
  end

  let(:server_selector) do
    double('server_selector', select_server: double('server'))
  end

  let(:retryable) do
    klass.new(operation, cluster)
  end

  before :each do
    allow(cluster).to receive(:servers).and_return([])
    allow(cluster).to receive(:max_read_retries).and_return(2)
    allow(cluster).to receive(:disconnect!)
    allow(cluster).to receive(:reconnect!)
    allow(cluster).to receive(:read_retry_interval).and_return(0.1)
  end

  describe '#read_with_retry' do

    context 'when no exception occurs' do

      before do
        expect(operation).to receive(:execute).and_return(true)
      end

      it 'executes the operation once' do
        expect(retryable.read).to be true
      end
    end

    context 'when a socket error occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::SocketError).ordered
        expect(cluster).to receive(:reconnect!).and_return(true).ordered
        #expect(cluster).to receive(:disconnect!).and_return(true).ordered
        allow(cluster).to receive(:max_read_retries).and_return(2)
        expect(cluster).to receive(:read_retry_interval).and_return(0.1).ordered
        allow(cluster).to receive(:servers).and_return([])
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it 'executes the operation twice' do
        expect(retryable.read).to be true
      end
    end

    context 'when a socket timeout error occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::SocketTimeoutError).ordered
        expect(cluster).to receive(:reconnect!).and_return(true).ordered
        #expect(cluster).to receive(:disconnect!).and_return(true).ordered
        allow(cluster).to receive(:max_read_retries).and_return(2)
        expect(cluster).to receive(:read_retry_interval).and_return(0.1).ordered
        allow(cluster).to receive(:servers).and_return([])
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it 'executes the operation twice' do
        expect(retryable.read).to be true
      end
    end

    context 'when an operation failure occurs' do

      context 'when the cluster is not a mongos' do

        before do
          expect(operation).to receive(:execute).and_raise(Mongo::Error::OperationFailure).ordered
          allow(cluster).to receive(:sharded?).and_return(false)
        end

        it 'raises an exception' do
          expect {
            retryable.read
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end

      context 'when the cluster is a mongos' do

        context 'when the operation failure is not retryable' do

          let(:error) do
            Mongo::Error::OperationFailure.new('cursor not found')
          end

          before do
            expect(operation).to receive(:execute).and_raise(error).ordered
            allow(cluster).to receive(:sharded?).and_return(true)
          end

          it 'raises the exception' do
            expect {
              retryable.read
            }.to raise_error(Mongo::Error::OperationFailure)
          end
        end

        context 'when there is an authentication failure' do

          context 'when it comes in via an OperationFailure' do

            let(:error) do
              Mongo::Error::OperationFailure.new('not authorized')
            end

            before do
              expect(operation).to receive(:execute).and_raise(error).ordered
              allow(cluster).to receive(:sharded?).and_return(true)
              allow(cluster).to receive(:reconnect!)
              allow(cluster).to receive(:max_read_retries).and_return(2)
              expect(cluster).to receive(:read_retry_interval).and_return(0.1).ordered
              expect(operation).to receive(:execute).and_return(true).ordered

              mock_context = double(Mongo::Server::Context)
              mock_connection = double(Mongo::Server::Connection)
              mock_server = double(Mongo::Server, :context => mock_context)
              expect(mock_context).to receive(:with_connection).and_yield(mock_connection)
              expect(cluster).to receive(:servers).at_least(:once).and_return([mock_server])
              allow(mock_server).to receive(:options)
              expect(mock_connection).to receive(:authenticate!)
            end

            it 're-authenticates and retries the operation' do
              expect(retryable.read).to be true
            end
          end

          context 'when it comes in via a Mongo::Auth::Unauthorized' do

            let(:error) do
              Struct.new("UnauthorizedTestUser", :name, :database)
              bad_user = Struct::UnauthorizedTestUser.new("foo", "test")
              Mongo::Auth::Unauthorized.new(bad_user)
            end

            before do
              expect(operation).to receive(:execute).and_raise(error).ordered
              allow(cluster).to receive(:sharded?).and_return(true)
              expect(cluster).to receive(:reconnect!).ordered
              allow(cluster).to receive(:max_read_retries).and_return(2)
              expect(cluster).to receive(:read_retry_interval).and_return(0.1).ordered
              expect(operation).to receive(:execute).and_return(true).ordered

              mock_context = double(Mongo::Server::Context)
              mock_connection = double(Mongo::Server::Connection)
              mock_server = double(Mongo::Server, :context => mock_context)
              expect(mock_context).to receive(:with_connection).and_yield(mock_connection)
              expect(cluster).to receive(:servers).at_least(:once).and_return([mock_server])
              allow(mock_server).to receive(:options)
              expect(mock_connection).to receive(:authenticate!)
            end

            it 're-authenticates and retries the operation' do
              expect(retryable.read).to be true
            end
          end
        end

        context 'when the operation failure is retryable' do

          let(:error) do
            Mongo::Error::OperationFailure.new('no master')
          end

          context 'when the retry succeeds' do

            before do
              allow(cluster).to receive(:disconnect!)
              allow(cluster).to receive(:reconnect!)
              expect(operation).to receive(:execute).and_raise(error).ordered
              allow(cluster).to receive(:sharded?).and_return(true)
              expect(cluster).to receive(:max_read_retries).and_return(1).ordered
              expect(cluster).to receive(:read_retry_interval).and_return(0.1).ordered
              expect(operation).to receive(:execute).and_return(true).ordered
            end

            it 'returns the result' do
              expect(retryable.read).to be true
            end
          end

          context 'when the retry fails once and then succeeds' do

            before do
              expect(operation).to receive(:execute).and_raise(error).ordered
              allow(cluster).to receive(:sharded?).and_return(true)
              expect(cluster).to receive(:max_read_retries).and_return(2).ordered
              expect(cluster).to receive(:read_retry_interval).and_return(0.1).ordered
              expect(operation).to receive(:execute).and_raise(error).ordered
              allow(cluster).to receive(:max_read_retries).and_return(3)
              expect(cluster).to receive(:read_retry_interval).and_return(0.1).ordered
              expect(operation).to receive(:execute).and_return(true).ordered
            end

            it 'returns the result' do
              expect(retryable.read).to be true
            end
          end
        end
      end
    end
  end

  describe '#write_with_retry' do

    context 'when no exception occurs' do

      before do
        expect(operation).to receive(:execute).and_return(true)
      end

      it 'executes the operation once' do
        expect(retryable.write).to be true
      end
    end

    context 'when a not master error occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::OperationFailure.new('not master')).ordered
        allow(cluster).to receive(:servers).and_return([])
        expect(cluster).to receive(:reconnect!).and_return(true).ordered
        allow(cluster).to receive(:max_read_retries).and_return(2)
        expect(cluster).to receive(:read_retry_interval).and_return(0.1).ordered

        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it 'executes the operation twice' do
        expect(retryable.write).to be true
      end
    end

    context 'when a not primary error occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::OperationFailure.new('Not primary'))
        allow(cluster).to receive(:servers).and_return([])
        allow(cluster).to receive(:max_read_retries).and_return(2)
        allow(cluster).to receive(:read_retry_interval).and_return(0.1)

        expect(operation).to receive(:execute).and_return(true)
      end

      it 'executes the operation twice' do
        expect(retryable.write).to be true
      end
    end

    context 'when a normal operation failure occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::OperationFailure).ordered
      end

      it 'raises an exception' do
        expect {
          retryable.write
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

  end
end
