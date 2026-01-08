using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Disruptor.Util;

namespace Disruptor.Processing;

/// <summary>
/// AOT-compatible BatchEventProcessor that does not use dynamic generic construction.
/// Slightly lower performance than the optimized version, but works correctly in AOT environments.
/// </summary>
public class AotBatchEventProcessor<T> : IEventProcessor<T>
    where T : class
{
    private readonly IDataProvider<T> _dataProvider;
    private readonly SequenceBarrier _sequenceBarrier;
    private readonly IBatchEventHandler<T> _eventHandler;
    private readonly Sequence _sequence = new();
    private readonly EventProcessorState _state;
    private IExceptionHandler<T> _exceptionHandler = new FatalExceptionHandler<T>();

    public AotBatchEventProcessor(
        IDataProvider<T> dataProvider,
        SequenceBarrier sequenceBarrier,
        IBatchEventHandler<T> eventHandler)
    {
        _dataProvider = dataProvider;
        _sequenceBarrier = sequenceBarrier;
        _eventHandler = eventHandler;
        _state = new EventProcessorState(sequenceBarrier, restartable: true);

        if (eventHandler is IEventProcessorSequenceAware sequenceAware)
            sequenceAware.SetSequenceCallback(_sequence);
    }

    public Sequence Sequence => _sequence;

    public Task Halt() => _state.Halt();

    public void Dispose() => _state.Dispose();

    public bool IsRunning => _state.IsRunning;

    public void SetExceptionHandler(IExceptionHandler<T> exceptionHandler)
    {
        _exceptionHandler = exceptionHandler ?? throw new ArgumentNullException(nameof(exceptionHandler));
    }

    public Task Start(TaskScheduler taskScheduler)
    {
        var runState = _state.Start();
        taskScheduler.StartLongRunningTask(() => Run(runState));
        return runState.StartTask;
    }

    private void Run(EventProcessorState.RunState runState)
    {
        NotifyStart(runState);
        try
        {
            ProcessEvents(runState.CancellationToken);
        }
        finally
        {
            NotifyShutdown(runState);
        }
    }

    [MethodImpl(Constants.AggressiveOptimization)]
    private void ProcessEvents(CancellationToken cancellationToken)
    {
        var nextSequence = _sequence.Value + 1L;
        var availableSequence = _sequence.Value;

        while (true)
        {
            try
            {
                var waitResult = _sequenceBarrier.WaitFor(nextSequence, cancellationToken);
                if (waitResult.IsTimeout)
                {
                    NotifyTimeout();
                    continue;
                }

                availableSequence = waitResult.UnsafeAvailableSequence;

                if (availableSequence >= nextSequence)
                {
                    var batch = _dataProvider.GetBatch(nextSequence, availableSequence);
                    _eventHandler.OnBatch(batch, nextSequence);
                    nextSequence += batch.Length;
                }

                _sequence.SetValue(nextSequence - 1);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                if (availableSequence >= nextSequence)
                {
                    var batch = _dataProvider.GetBatch(nextSequence, availableSequence);
                    _exceptionHandler.HandleEventException(ex, nextSequence, batch);
                    nextSequence += batch.Length;
                }

                _sequence.SetValue(nextSequence - 1);
            }
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void NotifyTimeout()
    {
        try
        {
            _eventHandler.OnTimeout(_sequence.Value);
        }
        catch (Exception ex)
        {
            _exceptionHandler.HandleOnTimeoutException(ex, _sequence.Value);
        }
    }

    private void NotifyStart(EventProcessorState.RunState runState)
    {
        try
        {
            _eventHandler.OnStart();
        }
        catch (Exception e)
        {
            _exceptionHandler.HandleOnStartException(e);
        }
        runState.OnStarted();
    }

    private void NotifyShutdown(EventProcessorState.RunState runState)
    {
        try
        {
            _eventHandler.OnShutdown();
        }
        catch (Exception e)
        {
            _exceptionHandler.HandleOnShutdownException(e);
        }
        runState.OnShutdown();
    }
}
