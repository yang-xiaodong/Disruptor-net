using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Disruptor.Util;

namespace Disruptor.Processing;

/// <summary>
/// AOT-compatible EventProcessor that does not use dynamic generic construction.
/// Slightly lower performance than the optimized version, but works correctly in AOT environments.
/// </summary>
public class AotEventProcessor<T> : IEventProcessor<T>
    where T : class
{
    private readonly IDataProvider<T> _dataProvider;
    private readonly SequenceBarrier _sequenceBarrier;
    private readonly IEventHandler<T> _eventHandler;
    private readonly Sequence _sequence = new();
    private readonly EventProcessorState _state;
    private IExceptionHandler<T> _exceptionHandler = new FatalExceptionHandler<T>();

    public AotEventProcessor(
        IDataProvider<T> dataProvider,
        SequenceBarrier sequenceBarrier,
        IEventHandler<T> eventHandler)
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

                var availableSequence = waitResult.UnsafeAvailableSequence;

                _eventHandler.OnBatchStart(availableSequence - nextSequence + 1);

                while (nextSequence <= availableSequence)
                {
                    var evt = _dataProvider[nextSequence];
                    _eventHandler.OnEvent(evt, nextSequence, nextSequence == availableSequence);
                    nextSequence++;
                }

                _sequence.SetValue(availableSequence);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                var evt = _dataProvider[nextSequence];
                _exceptionHandler.HandleEventException(ex, nextSequence, evt);
                _sequence.SetValue(nextSequence);
                nextSequence++;
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
