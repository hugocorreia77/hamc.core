namespace Hamc.Core.Tasks
{
    /// <summary>
    /// Manages a list of items and devide them into buckets. It allows to apply function tasks over the small buckets of items in bulk.
    /// </summary>
    /// <typeparam name="T">Type of objects that will be managed.</typeparam>
    /// <typeparam name="Y">Type of the output expected by the function that will be applied.</typeparam>
    public class TaskBatch<T,Y>
    {
        private readonly int _batchSize;
        private readonly IEnumerable<T> _items;
        private IEnumerable<IEnumerable<T>> _batches;

        /// <summary>
        /// Manages a list of items and devide them into buckets. It allows to apply function tasks over the small buckets of items in bulk.
        /// </summary>
        /// <param name="items">List of objects to split into batches.</param>
        /// <param name="batchSize">The amount of items per batch.</param>
        /// <exception cref="ArgumentException"></exception>
        public TaskBatch(IEnumerable<T> items, int batchSize = 10)
        {
            if (items is null || !items.Any())
            {
                throw new ArgumentException("Items were not provided.");
            }
            if(batchSize <= 0)
            {
                throw new ArgumentException("Batch size should be greater than zero.");
            }

            _items = items;
            _batchSize = batchSize;
            _batches = [];
        }

        /// <summary>
        /// It applies a function to each item of each bucket. The cancellation token, if used, will interrupt the process between buckets.
        /// </summary>
        /// <param name="taskToExecute">The function task to apply to each item.</param>
        /// <param name="cancellationToken">The cancellation token that allows the interuption of the process.</param>
        /// <returns>A collection of results, one for each task execution using the type Y.</returns>
        public virtual async Task<IEnumerable<Y>> ResolveAsync(Func<T, Task<Y>> taskToExecute, CancellationToken cancellationToken)
        {
            _batches = GetBatches(_items, _batchSize);
            var results = new List<Y>();
            foreach (var batch in _batches) 
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return results;
                }

                var batchTask = new List<Task<Y>>();
                foreach(var itemFromBatch in batch)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        return results;
                    }
                    var t = taskToExecute(itemFromBatch);
                    batchTask.Add(t);
                }

                await Task.WhenAll(batchTask);

                results.AddRange(batchTask.Select(t => t.Result).ToList());
            }

            return await Task.FromResult(results);
        }

        /// <summary>
        /// It applies a function to each item of each bucket and returns the result of each task as soon as it is ready.
        /// </summary>
        /// <param name="taskToExecute">The function task to apply to each item.</param>
        /// <param name="cancellationToken">The cancellation token that allows the interuption of the process.</param>
        /// <returns></returns>
        public virtual async IAsyncEnumerable<Task<Y>> ResolveWithResultsAsync(Func<T, Task<Y>> taskToExecute, CancellationToken cancellationToken)
        {
            _batches = GetBatches(_items, _batchSize);

            foreach (var batch in _batches)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                var batchTask = new List<Task<Y>>();
                foreach (var itemFromBatch in batch)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                    var t = taskToExecute(itemFromBatch);
                    batchTask.Add(t);
                }

                await Task.WhenAll(batchTask);

                foreach (var taskFinished in batchTask)
                {
                    yield return taskFinished;
                }
            }
        }

        internal static IEnumerable<IEnumerable<Z>> GetBatches<Z>(IEnumerable<Z> items, int batchSize)
        {
            if (items is null || !items.Any())
            {
                throw new ArgumentException("Items were not provided.");
            }
            if (batchSize <= 0)
            {
                throw new ArgumentException("Batch size should be greater than zero.");
            }

            var batches = new List<IEnumerable<Z>>();
            for(int i=0; i< items.Count(); i+=batchSize) 
            {
                batches.Add(items.ToList().GetRange(i, Math.Min(batchSize, items.Count()-i)));
            }
            return batches;
        }

    }
}
